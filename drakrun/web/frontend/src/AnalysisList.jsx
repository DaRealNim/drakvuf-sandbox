import { Link } from "react-router-dom";
import { useEffect, useState } from "react";
import { getAnalysisList } from "./api";
import { CanceledError } from "axios";
import { AnalysisStatusBadge } from "./AnalysisStatusBadge.jsx";
import { AnalysisPriorityBadge } from "./AnalysisPriorityBadge.jsx";
import { formatDate } from "./formatUtils.js";

const STATES = ["queued", "started", "finished"];
const PAGE_SIZE = 50;

function AnalysisListRow({ analysis }) {
    return (
        <tr>
            <td>
                <AnalysisStatusBadge status={analysis.status} />
                <AnalysisPriorityBadge priority={analysis.priority} />
                <Link to={`/analysis/${analysis.id}`}>{analysis.id}</Link>
            </td>
            <td>
                <div className="d-flex flex-row flex-wrap font-monospace">
                    <div className="fw-bold pe-2">SHA256:</div>
                    <div>{analysis.file.sha256}</div>
                </div>
                <div className="d-flex flex-row flex-wrap font-monospace">
                    <div className="fw-bold pe-2">Name:</div>
                    <div>{analysis.file.name}</div>
                </div>
                <div className="d-flex flex-row flex-wrap">
                    <div className="fw-bold pe-2">Type:</div>
                    <div>{analysis.file.type}</div>
                </div>
            </td>
            <td>{formatDate(analysis.time_started)}</td>
            <td>{formatDate(analysis.time_finished)}</td>
        </tr>
    );
}

function AnalysisListTable({ state, page, onTotalChange }) {
    const [error, setError] = useState();
    const [analysisList, setAnalysisList] = useState();

    useEffect(() => {
        const abortController = new AbortController();
        setAnalysisList(undefined);
        getAnalysisList({ state, page, limit: PAGE_SIZE, abortController })
            .then((response) => {
                setAnalysisList(response.items);
                onTotalChange(response.total);
            })
            .catch((error) => {
                if (!(error instanceof CanceledError)) {
                    setError(error);
                    console.error(error);
                }
            });
        return () => {
            abortController.abort();
        };
    }, [state, page]);

    if (typeof error !== "undefined") {
        return <div>Error: {error.toString()}</div>;
    }

    if (typeof analysisList === "undefined") {
        return <div>Loading...</div>;
    }

    if (analysisList.length === 0) {
        return <div>There are no analyses in this view.</div>;
    }

    return (
        <div className="datatable-container">
            <table className="datatable-table">
                <thead>
                    <tr>
                        <th>Analysis ID</th>
                        <th>Sample info</th>
                        <th>Started</th>
                        <th>Finished</th>
                    </tr>
                </thead>
                <tbody>
                    {analysisList.map((analysis) => (
                        <AnalysisListRow
                            analysis={analysis}
                            key={analysis.id}
                        />
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export default function AnalysisList() {
    const [state, setState] = useState("queued");
    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

    const switchState = (newState) => {
        setState(newState);
        setPage(1);
    };

    return (
        <div className="container-fluid px-4">
            <h1 className="m-4 h4">Analyses</h1>
            <ul className="nav nav-tabs mb-3">
                {STATES.map((s) => (
                    <li className="nav-item" key={s}>
                        <button
                            type="button"
                            className={
                                "nav-link" + (state === s ? " active" : "")
                            }
                            onClick={() => switchState(s)}
                        >
                            {s.charAt(0).toUpperCase() + s.slice(1)}
                        </button>
                    </li>
                ))}
            </ul>
            <AnalysisListTable
                state={state}
                page={page}
                onTotalChange={setTotal}
            />
            <div className="d-flex align-items-center justify-content-between mt-3">
                <button
                    type="button"
                    className="btn btn-outline-secondary btn-sm"
                    disabled={page <= 1}
                    onClick={() => setPage((p) => p - 1)}
                >
                    Previous
                </button>
                <span>
                    Page {page} of {totalPages}
                </span>
                <button
                    type="button"
                    className="btn btn-outline-secondary btn-sm"
                    disabled={page >= totalPages}
                    onClick={() => setPage((p) => p + 1)}
                >
                    Next
                </button>
            </div>
        </div>
    );
}
