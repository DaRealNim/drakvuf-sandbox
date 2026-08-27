import datetime
import logging
import shutil
import socket
from typing import List, Optional, Tuple

from redis import Redis
from rq import Queue, Worker, get_current_job
from rq.job import Job

from drakrun.lib.config import RedisConfigSection, load_config
from drakrun.lib.paths import ANALYSES_DIR, UPLOADS_DIR

from ..lib.s3_storage import (
    download_sample_from_s3,
    get_s3_client,
    is_s3_enabled,
    upload_analysis,
)
from .analysis_metadata import AnalysisMetadata, FileMetadata
from .analysis_options import AnalysisOptions, JobPriority
from .analyzer import AnalysisSubstatus, analyze_file

ANALYSIS_QUEUE_NAMES = {
    "high": "drakrun-analysis-high",
    "normal": "drakrun-analysis-normal",
    "low": "drakrun-analysis-low",
}
ANALYSIS_HISTORY_KEY = "drakrun-analysis-history"
_WORKER_VM_ID: Optional[int] = None

logger = logging.getLogger(__name__)


def get_redis_connection(config: RedisConfigSection):
    redis = Redis(
        host=config.host,
        port=config.port,
        username=config.username,
        password=config.password,
    )
    return redis


def analysis_job_to_metadata(job: Job) -> AnalysisMetadata:
    job_status = job.get_status()
    job_meta = job.get_meta()
    time_finished = job.meta["time_finished"] if "time_finished" in job.meta else None
    if time_finished is None:
        time_finished = job.ended_at.isoformat() if job.ended_at is not None else None
    return AnalysisMetadata.load_from_dict(
        {
            **job_meta.get("extra_metadata", {}),
            "id": job.id,
            "status": job_status.value if job_status is not None else "unknown",
            "substatus": job_meta.get("substatus"),
            "options": job_meta["options"],
            "file": job_meta["file"],
            "vm_id": job_meta.get("vm_id"),
            "priority": job_meta.get("priority", "normal"),
            "time_started": (
                job.started_at.isoformat() if job.started_at is not None else None
            ),
            "time_execution_started": (
                job.meta["time_execution_started"]
                if "time_execution_started" in job.meta
                else None
            ),
            "time_finished": time_finished,
        }
    )


def worker_analyze(options: AnalysisOptions):
    if _WORKER_VM_ID is None:
        raise RuntimeError("Fatal error: no vm_id assigned in worker")

    config = load_config()
    if is_s3_enabled(config.s3):
        s3_client = get_s3_client(config.s3)
        s3_bucket = config.s3.bucket
    else:
        s3_client = None
        s3_bucket = None

    # Reconstruct options object to include worker-side preset defaults
    options = AnalysisOptions.with_config_defaults(config, **dict(options))

    if not options.plugins:
        raise RuntimeError("Cannot analyze sample without plugins specified")

    job = get_current_job()
    job.meta["vm_id"] = _WORKER_VM_ID
    job.save_meta()

    file_metadata = FileMetadata.model_validate(job.meta["file"])

    vm_id = _WORKER_VM_ID
    output_dir = ANALYSES_DIR / job.id
    output_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(output_dir / "drakrun.log")
    formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s] %(message)s")
    file_handler.setFormatter(formatter)
    drakrun_logger = logging.getLogger("drakrun")
    drakrun_logger.addHandler(file_handler)

    metadata = AnalysisMetadata(
        id=job.id,
        options=options,
        time_started=job.started_at.isoformat(),
        vm_id=vm_id,
        file=file_metadata,
    )
    metadata_file = output_dir / "metadata.json"
    metadata.store_to_file(metadata_file)

    def substatus_callback(substatus: AnalysisSubstatus, updated_options: bool = False):
        job.meta["substatus"] = substatus.value
        if substatus == AnalysisSubstatus.analyzing:
            metadata.time_execution_started = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat()
            job.meta["time_execution_started"] = metadata.time_execution_started
        if updated_options:
            job.meta["options"] = options.to_dict(exclude_none=True)
        job.save_meta()

    if options.host_sample_path is None:
        if s3_client is None:
            raise RuntimeError("Got sample referenced on S3 but S3 is not enabled")
        # Sample is passed via S3
        UPLOADS_DIR.mkdir(exist_ok=True)
        upload_path = UPLOADS_DIR / f"{job.id}.sample"
        download_sample_from_s3(job.id, upload_path, s3_client, s3_bucket)
        options.host_sample_path = upload_path

    job_success = True
    try:
        extra_metadata = analyze_file(
            vm_id, output_dir, metadata, substatus_callback=substatus_callback
        )
        job.meta.update({"extra_metadata": extra_metadata})
        metadata.model_extra.update(extra_metadata)
        job.save_meta()
    except BaseException:
        job_success = False
        logger.exception("Failed to analyze sample")
        raise
    finally:
        drakrun_logger.removeHandler(file_handler)
        file_handler.close()

        metadata.status = "finished" if job_success else "failed"
        finished_at = datetime.datetime.now(datetime.timezone.utc)
        metadata.time_finished = finished_at.isoformat()
        job.meta["time_finished"] = metadata.time_finished
        job.save_meta()
        job.connection.zadd(ANALYSIS_HISTORY_KEY, {job.id: finished_at.timestamp()})

        metadata.store_to_file(metadata_file)
        options.host_sample_path.unlink()
        if s3_client is not None:
            upload_analysis(job.id, output_dir, s3_client, s3_bucket)
            if config.s3.remove_local_after_upload:
                shutil.rmtree(output_dir)


def worker_main(vm_id: int):
    global _WORKER_VM_ID
    _WORKER_VM_ID = vm_id
    config = load_config()
    hostname = config.drakrun.worker_hostname or socket.gethostname()

    worker = Worker(
        queues=list(ANALYSIS_QUEUE_NAMES.values()),
        name=f"drakrun-worker@{hostname}:vm-{vm_id}",
        connection=get_redis_connection(config.redis),
    )
    worker.work()


def enqueue_analysis(
    job_id: str,
    file_metadata: FileMetadata,
    options: AnalysisOptions,
    connection: Redis,
    result_ttl: int,
    priority: JobPriority = "normal",
) -> Job:
    queue_name = ANALYSIS_QUEUE_NAMES[priority]
    queue = Queue(name=queue_name, connection=connection)
    if options.timeout is None:
        raise RuntimeError("Timeout is required when spawning analysis to worker")
    return queue.enqueue(
        worker_analyze,
        options,
        job_id=job_id,
        meta={
            "options": options.to_dict(exclude_none=True),
            "file": file_metadata.to_dict(),
            "priority": priority,
        },
        job_timeout=options.timeout + options.job_timeout_leeway,
        result_ttl=result_ttl,
    )


def get_queued_analyses(
    connection: Redis, offset: int = 0, limit: int = 50
) -> Tuple[List[Job], int]:
    # Queues are listed high -> low, so pagination naturally follows dispatch order.
    queues = [Queue(name=name, connection=connection) for name in ANALYSIS_QUEUE_NAMES.values()]
    total = sum(queue.count for queue in queues)

    jobs: List[Job] = []
    remaining_offset = offset
    remaining_limit = limit
    for queue in queues:
        if remaining_limit <= 0:
            break
        queue_count = queue.count
        if remaining_offset >= queue_count:
            remaining_offset -= queue_count
            continue
        take = min(remaining_limit, queue_count - remaining_offset)
        jobs.extend(queue.get_jobs(offset=remaining_offset, length=take))
        remaining_offset = 0
        remaining_limit -= take
    return jobs, total


def get_started_analyses(
    connection: Redis, offset: int = 0, limit: int = 50
) -> Tuple[List[Job], int]:
    # Bounded by worker count, so fetching everything and sorting in Python is cheap.
    jobs: List[Job] = []
    for queue_name in ANALYSIS_QUEUE_NAMES.values():
        queue = Queue(name=queue_name, connection=connection)
        job_ids = queue.started_job_registry.get_job_ids()
        jobs.extend(
            job
            for job in Job.fetch_many(job_ids, connection=connection)
            if job is not None
        )
    jobs.sort(key=lambda job: job.started_at or job.enqueued_at, reverse=True)
    return jobs[offset : offset + limit], len(jobs)


def get_finished_analyses(
    connection: Redis, offset: int = 0, limit: int = 50
) -> Tuple[List[Job], int]:
    total = connection.zcard(ANALYSIS_HISTORY_KEY)
    job_ids = [
        job_id.decode() if isinstance(job_id, bytes) else job_id
        for job_id in connection.zrevrange(
            ANALYSIS_HISTORY_KEY, offset, offset + limit - 1
        )
    ]
    jobs = [
        job for job in Job.fetch_many(job_ids, connection=connection) if job is not None
    ]
    return jobs, total
