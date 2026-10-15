import json
import logging
import shutil
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from botocore.exceptions import ClientError
from redis import Redis
from rq.exceptions import NoSuchJobError
from rq.job import Job, JobStatus

from drakrun.lib.config import CleanupCriteriaSection, S3StorageConfigSection
from drakrun.lib.paths import ANALYSES_DIR
from drakrun.lib.s3_storage import delete_analysis_from_s3, get_s3_client, is_s3_enabled

from .analysis_options import JobPriority
from .worker import ANALYSIS_HISTORY_KEY

logger = logging.getLogger(__name__)

# Cross-process lock (web workers + the vm-1 scheduler thread all share one
# Redis instance) so a manual trigger and the scheduled run never execute
# concurrently. TTL is a safety net in case a run dies without releasing it.
CLEANUP_LOCK_KEY = "drakrun-cleanup-lock"
CLEANUP_LOCK_TTL = 3600

_JOB_STATUS_TO_CLEANUP_STATUS = {
    JobStatus.FINISHED: "finished",
    JobStatus.FAILED: "failed",
}


@dataclass
class CleanupCandidate:
    job_id: str
    status: str
    priority: Optional[JobPriority]


@dataclass
class CleanupReport:
    dry_run: bool = False
    deleted: Dict[str, int] = field(default_factory=dict)
    # Analyses that matched status/age but were skipped because their priority
    # couldn't be determined from either the live Redis job or metadata.json
    # (only relevant when a priority filter is set)
    unknown_priority_skipped: int = 0
    # History entries whose status couldn't be determined at all (no live job,
    # no metadata.json) are left untouched
    orphaned_skipped: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def total_deleted(self) -> int:
        return sum(self.deleted.values())

    def to_dict(self) -> dict:
        return {
            "dry_run": self.dry_run,
            "deleted": self.deleted,
            "total_deleted": self.total_deleted,
            "unknown_priority_skipped": self.unknown_priority_skipped,
            "orphaned_skipped": self.orphaned_skipped,
            "errors": self.errors,
        }


def _decode_ids(raw_ids) -> List[str]:
    return [raw_id.decode() if isinstance(raw_id, bytes) else raw_id for raw_id in raw_ids]


def _read_metadata(
    analysis_id: str, s3_config: Optional[S3StorageConfigSection]
) -> Optional[dict]:
    if not is_s3_enabled(s3_config):
        path = ANALYSES_DIR / analysis_id / "metadata.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())
    s3_client = get_s3_client(s3_config)
    key = "/".join([*analysis_id[0:4], analysis_id, "metadata.json"])
    try:
        data = s3_client.get_object(Bucket=s3_config.bucket, Key=key)["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(data)


def _resolve_status_and_priority(
    job_id: str, redis: Redis, s3_config: Optional[S3StorageConfigSection]
) -> Tuple[Optional[str], Optional[JobPriority]]:
    try:
        job = Job.fetch(job_id, connection=redis)
    except NoSuchJobError:
        job = None

    if job is not None:
        status = _JOB_STATUS_TO_CLEANUP_STATUS.get(job.get_status())
        if status is not None:
            return status, job.meta.get("priority")

    # Redis job is gone (evicted via result_ttl, or already deleted some other
    # way): fall back to the metadata.json written alongside the results.
    metadata = _read_metadata(job_id, s3_config)
    if metadata is None:
        return None, None
    return metadata.get("status"), metadata.get("priority")


def find_cleanup_candidates(
    redis: Redis,
    criteria: CleanupCriteriaSection,
    s3_config: Optional[S3StorageConfigSection],
) -> Tuple[List[CleanupCandidate], int, int]:
    """
    Returns (candidates, unknown_priority_skipped, orphaned_skipped).

    Only ever considers analyses already recorded in ANALYSIS_HISTORY_KEY,
    which is populated exclusively once an analysis has fully finished or
    failed - queued/started analyses can never appear here, so there is no
    risk of matching an analysis that is still running.
    """
    now = time.time()
    max_score = now - criteria.older_than if criteria.older_than is not None else "+inf"
    job_ids = _decode_ids(redis.zrangebyscore(ANALYSIS_HISTORY_KEY, "-inf", max_score))

    candidates: List[CleanupCandidate] = []
    unknown_priority_skipped = 0
    orphaned_skipped = 0

    for job_id in job_ids:
        status, priority = _resolve_status_and_priority(job_id, redis, s3_config)
        if status is None:
            orphaned_skipped += 1
            continue
        if criteria.statuses and status not in criteria.statuses:
            continue
        if criteria.priorities:
            if priority is None:
                unknown_priority_skipped += 1
                continue
            if priority not in criteria.priorities:
                continue
        candidates.append(
            CleanupCandidate(job_id=job_id, status=status, priority=priority)
        )

    return candidates, unknown_priority_skipped, orphaned_skipped


def _delete_analysis(
    job_id: str, redis: Redis, s3_config: Optional[S3StorageConfigSection]
) -> None:
    try:
        job = Job.fetch(job_id, connection=redis)
        job.delete(remove_from_queue=True)
    except NoSuchJobError:
        pass
    redis.zrem(ANALYSIS_HISTORY_KEY, job_id)

    analysis_dir = ANALYSES_DIR / job_id
    if analysis_dir.exists():
        shutil.rmtree(analysis_dir)

    if is_s3_enabled(s3_config):
        s3_client = get_s3_client(s3_config)
        delete_analysis_from_s3(job_id, s3_client, s3_config.bucket)


def perform_cleanup(
    redis: Redis,
    s3_config: Optional[S3StorageConfigSection],
    criteria: CleanupCriteriaSection,
    dry_run: bool = False,
) -> CleanupReport:
    candidates, unknown_priority_skipped, orphaned_skipped = find_cleanup_candidates(
        redis, criteria, s3_config
    )
    report = CleanupReport(
        dry_run=dry_run,
        unknown_priority_skipped=unknown_priority_skipped,
        orphaned_skipped=orphaned_skipped,
    )

    for candidate in candidates:
        if not dry_run:
            try:
                _delete_analysis(candidate.job_id, redis, s3_config)
            except Exception as e:
                logger.exception("Failed to clean up analysis %s", candidate.job_id)
                report.errors.append(f"{candidate.job_id}: {e}")
                continue
        report.deleted[candidate.status] = report.deleted.get(candidate.status, 0) + 1

    return report


def try_run_cleanup(
    redis: Redis,
    s3_config: Optional[S3StorageConfigSection],
    criteria: CleanupCriteriaSection,
    dry_run: bool = False,
) -> Optional[CleanupReport]:
    """
    Like perform_cleanup, but first acquires CLEANUP_LOCK_KEY so a manual
    trigger and the scheduled run can't execute at the same time. Returns
    None (without doing anything) if a cleanup is already in progress.
    """
    if not redis.set(CLEANUP_LOCK_KEY, "1", nx=True, ex=CLEANUP_LOCK_TTL):
        return None
    try:
        return perform_cleanup(redis, s3_config, criteria, dry_run=dry_run)
    finally:
        redis.delete(CLEANUP_LOCK_KEY)
