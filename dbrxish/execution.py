from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language, ClusterDetails, Results
from databricks.sdk.service.sql import StatementResponse
from databricks.sdk.service.catalog import ColumnInfo
from databricks.sdk.service._internal import Wait
from pyspark.sql import DataFrame, SparkSession
from datetime import timedelta, datetime
from pathlib import Path
from dataclasses import dataclass
from functools import reduce
from typing import Any
import csv
import json
import time


# handles table output for the sql editor
@dataclass
class QueryResult:
    header: list[str]
    rows: list[list[str | int | float | bool | None]]
    preview: int = 100

    def csv(self, f: str | Path):
        with open(f, "w") as fc:
            writer = csv.writer(fc, delimiter=",")
            writer.writerow(self.header)
            writer.writerows(self.rows)

    def __add__(self, qr: "QueryResult") -> Any:
        if qr.header == self.header:
            return QueryResult(self.header, self.rows + qr.rows)

    def display(self, limit: int | None = None) -> str:
        max_lens = []
        out = []
        for idx in range(len(self.header)):
            max_lens.append(
                max([len(str(r[idx])) + 2 for r in [self.header] + self.rows])
            )
        for idx, row in enumerate([self.header] + self.rows):
            out.append(
                " ".join(
                    [str(r).ljust(max_lens[idx] or 0) for idx, r in enumerate(row)]
                )
            )
            if idx == limit:
                return "\n".join(out)
        return "\n".join(out)

    def __repr__(self):
        overage = len(self.rows) - self.preview
        output = self.display(limit=self.preview)
        if overage > 0:
            output = output + f"\n... ({overage} more rows)"
        return output

    def to_df(self, spark: SparkSession) -> DataFrame:
        return spark.createDataFrame(
            self.rows,
            schema=self.header
        )


# allows sql editor-esque functionality in local code/repl
class SQLEditor:
    def __init__(
        self,
        wc: WorkspaceClient | None = None,
        warehouse_id: str | None = None,
    ):
        self.wc = wc or WorkspaceClient()
        self.warehouse_id = warehouse_id or self.wc.config.warehouse_id
        self.history = []

    def _convert(
        self, results: list[list[str]], columns: list[ColumnInfo]
    ) -> QueryResult:
        cols = sorted(
            [
                (p.position, "STRING" if not p.type_name else p.type_name.value)
                for p in columns
            ]
        )
        header = [r[1] for r in sorted([(p.position, p.name) for p in columns])]
        converted = []
        for row in results:
            record = []
            for k, v in cols:
                if k is not None:
                    match v:
                        case "BINARY" | "INT" | "SHORT" | "LONG":
                            record.append(int(row[k]))
                        case "DECIMAL" | "DOUBLE" | "FLOAT" | "ARRAY":
                            record.append(float(row[k]))
                        case "NULL":
                            record.append(None)
                        case "BOOLEAN":
                            record.append(
                                True if row[k].lower().startswith("t") else False
                            )
                        case _:
                            record.append(row[k])
                else:
                    record.append(None)
            converted.append(record)
        return QueryResult(header, converted)

    def query(self, query: str) -> QueryResult | None:
        response = self.wc.statement_execution.execute_statement(
            query, self.warehouse_id, wait_timeout="50s"
        )
        try:
            table = self.collect(response)
        except Exception as e:
            self.history.append((query, response.statement_id, e))
            raise e
        else:
            self.history.append((query, response.statement_id, table))
        return table

    def collect(self, response: StatementResponse) -> QueryResult | None:
        table_chunks = []
        if response is not None and response.statement_id:
            if response.status and not response.status.error:
                r = self.wc.statement_execution.get_statement(response.statement_id)
                while (
                    r
                    and r.status
                    and r.status.state
                    and r.status.state.value in ["PENDING", "RUNNING"]
                ):
                    time.sleep(5)
                    r = self.wc.statement_execution.get_statement(response.statement_id)
                if r.result:
                    found_columns = None
                    if r.manifest and r.manifest.schema:
                        found_columns = r.manifest.schema.columns
                    if r.result.data_array:
                        if found_columns:
                            table_chunks.append(
                                self._convert(r.result.data_array, found_columns)
                            )
                    if r.result.next_chunk_index:
                        found_statement_id = r.statement_id
                        if found_statement_id and found_columns:
                            chunk = self.wc.statement_execution.get_statement_result_chunk_n(
                                found_statement_id, r.result.next_chunk_index
                            )
                            while chunk and chunk.next_chunk_index and chunk.data_array:
                                table_chunks.append(
                                    self._convert(chunk.data_array, found_columns)
                                )
                                chunk = self.wc.statement_execution.get_statement_result_chunk_n(
                                    found_statement_id, chunk.next_chunk_index
                                )
                    if table_chunks:
                        table = reduce(lambda a, b: a + b, table_chunks)
                    else:
                        table = QueryResult([], [])
                    table = (
                        QueryResult([], [])
                        if not table_chunks
                        else reduce(lambda a, b: a + b, table_chunks)
                    )
                    return table
            else:
                if response.status and response.status.error:
                    if (
                        response.status.error.error_code
                        and response.status.error.message
                    ):
                        raise Exception(
                            f"{response.status.error.error_code.value} - {response.status.error.message}"
                        )
                    else:
                        raise Exception(f"Unknown error: {response.status.error}")
                else:
                    raise Exception("Unknown error reading query results")

    def __call__(self, query: str) -> QueryResult | None:
        return self.query(query)

    def __sub__(self, query: str):
        return self.__call__(query)

    @property
    def table(self) -> QueryResult | None:
        if self.history:
            return self.history[-1][-1]


# just an easy helper function to return the databricks sdk enum for a lang
def get_language(language: str) -> Language:
    if language is not None:
        match language.lower():
            case "scala":
                return Language.SCALA
            case "sql":
                return Language.SQL
            case _:
                return Language.PYTHON


# handles statement remote code execution from local repl
#
# TODO: this needs to get cleaned up.  the cluster startup/management code should be
# in a separate class/module and this should just handle remote code execution
class ExecutionKernel:
    def __init__(
        self,
        wc: WorkspaceClient,
        cluster_id: str | None = None,
        verbose: bool = False,
        default_language: Language = Language.PYTHON,
        cluster_timeout_mins: int = 20,
        context_timeout_mins: int = 5,
        command_timeout_mins: int = 20,
        wait_for_session: bool = False,
        check_status_interval_mins: int = 5,
    ):
        self.wc = wc
        self.verbose = verbose
        self.cluster_id = cluster_id or wc.config.cluster_id
        self.cluster_timeout = timedelta(minutes=cluster_timeout_mins) or timedelta(
            minutes=20
        )
        self.context_timeout = timedelta(minutes=context_timeout_mins) or timedelta(
            minutes=5
        )
        self.command_timeout = timedelta(minutes=command_timeout_mins) or timedelta(
            minutes=60
        )
        self.check_status_interval_mins = check_status_interval_mins or 30
        self._status_time = datetime.now()
        self._session_ready = False
        self.wait_for_session = wait_for_session
        self.default_language = default_language
        self._context_id = self._context(wait=self.wait_for_session)

    def _dbg(self, msg: str, error: bool = False):
        if self.verbose:
            status = "error" if error else "info"
            print(f"[{status}] {msg}")

    def _start_cluster(self, wait: bool = False) -> Wait | None:
        self._dbg(f"Cluster state: {self.cluster_state}")
        match self.cluster_state:
            case "TERMINATED" | "TERMINATING":
                self._dbg("Starting cluster")
                return self._cluster_action("start", wait)
            case "ERROR" | "UNKNOWN":
                try:
                    self._dbg("Restarting cluster")
                    return self._cluster_action("restart", wait)
                except Exception as e:
                    self._dbg(
                        f"Failed to restart cluster, will try start.  Cause: {e}",
                        error=True,
                    )
                    return self._cluster_action("start", wait)

    def _cluster_action(self, action: str, wait: bool = False) -> Wait | None:
        match action.lower():
            case "start":
                call = self.wc.clusters.start(self.cluster_id)
                if wait:
                    return call.result(timeout=self.cluster_timeout)
                return call
            case "restart":
                call = self.wc.clusters.restart(self.cluster_id)
                if wait:
                    return call.result(timeout=self.cluster_timeout)
                return call
            case "resize":
                call = self.wc.clusters.resize(self.cluster_id)
                if wait:
                    return call.result(timeout=self.cluster_timeout)
                return call
            case "terminate":
                call = self.wc.clusters.delete(self.cluster_id)
                if wait:
                    return call.result(timeout=self.cluster_timeout)
                return call
        return None

    @property
    def cluster(self) -> ClusterDetails:
        self._dbg(f"Getting cluster details for {self.cluster_id}")
        try:
            return self.wc.clusters.get(self.cluster_id)
        except Exception as e:
            raise AttributeError(f"Cluster {self.cluster_id} not found.  Cause: {e}")

    def quit_context(self, wait: bool = False) -> Wait | None:
        if self.context_id:
            result = self.wc.command_execution.cancel(
                context_id=self.context_id, cluster_id=self.cluster_id
            )
            if wait:
                return result.result(timeout=self.context_timeout)
            return result

    @property
    def cluster_ready(self) -> bool:
        return self.cluster_state == "RUNNING"

    def _context(self, wait: bool = False) -> str | None:
        context_id = None
        self._dbg("Getting context")
        if not self.cluster_ready:
            self._start_cluster(wait=wait)
        if self.cluster_ready:
            self._dbg("Cluster is ready, getting context")
            ctx = self.wc.command_execution.create(
                cluster_id=self.cluster_id, language=self.default_language
            )
            context_id = ctx.bind().get("context_id")
            if wait:
                self._dbg("Waiting for context to be ready")
                ctx = ctx.result(timeout=self.context_timeout)
        return context_id

    @property
    def context_id(self) -> str | None:
        return self._context_id or self._context()

    @property
    def clean_context(self, wait: bool = False) -> str | None:
        self._dbg("Cleaning up context")
        self.quit_context(wait=False)
        self._dbg("Creating new context")
        self._context_id = self._context(wait=wait)
        return self.context_id

    @property
    def session_ready(self) -> bool:
        self._session_ready = bool(self.cluster_ready and self.context_ready)
        return self._session_ready

    @property
    def session_check(self) -> bool:
        if (
            (datetime.now() - self._status_time).seconds / 60
            > self.check_status_interval_mins
        ) or not self._session_ready:
            self._status_time = datetime.now()
            self._session_ready = self.session_ready
        return self._session_ready

    @property
    def cluster_state(self) -> str:
        state = "_UNKNOWN_"
        try:
            state = self.cluster.state.value.upper()
        except Exception as e:
            state = f"_ERROR_[{e}]"
        finally:
            self._dbg(f"Cluster state: {state}")
            return state

    @property
    def cluster_status_message(self) -> str:
        summary = "_NONE_"
        state = self.cluster_state
        msg = f" state - {state} | status - {summary}"
        try:
            summary = self.cluster.state_message
        except Exception as e:
            summary = f"_ERROR_[{e}]"
        msg = f"state - {state} | status - {summary}"
        self.current_cluster_status = msg
        self._dbg(msg)
        return msg

    @property
    def context_state(self) -> str:
        if self.context_id:
            try:
                status = self.wc.command_execution.context_status(
                    context_id=self.context_id, cluster_id=self.cluster_id
                ).status
                if status:
                    return status.value.upper()
            except Exception as e:
                return f"_ERROR_[{e}]"
        return "_NO_CONTEXT_"

    @property
    def context_ready(self) -> bool:
        return self.context_state == "RUNNING"

    @property
    def session_state(self) -> str:
        if self.cluster_state != "RUNNING":
            return f"CLUSTER_{self.cluster_state}"
        return f"CONTEXT_{self.context_state}"

    @property
    def session_status_message(self) -> str:
        if self.cluster_state != "RUNNING":
            msg = f"Waiting on cluster: {self.cluster_status_message}"
        elif self.context_state != "RUNNING":
            msg = f"Waiting on context: status - {self.context_state}"
        else:
            msg = "Session is ready"
        return msg

    def _execute(
        self, cmd: str, language: None | Language = None
    ) -> tuple[Wait | None, str | None]:
        result, msg = None, None
        if self.session_check:
            self._dbg(f"Executing command: {cmd}")
            result = self.wc.command_execution.execute(
                cluster_id=self.cluster_id,
                command=cmd,
                context_id=self.context_id,
                language=(language or self.default_language),
            )
        else:
            msg = f"Session not ready. {self.session_status_message}"
        return result, msg

    def execute(
        self,
        cmd: str,
        language: Language | None = None,
        show: bool = True,
        results: bool = False,
    ) -> tuple[None | Results, None | str] | str | None:
        result, msg = self._execute(cmd, language)
        if result:
            result = result.result(timeout=self.command_timeout)
            if result.status:
                if result.status.value.lower() == "error":
                    msg = f"Error executing command: {result.results}"
                    if result.results:
                        msg = f"No output returned.  Cause:\n{result.results.cause}\n\nSummary: {result.results.summary}"
            if result.results:
                out = result.results
                if out.data:
                    if out.is_json_schema:
                        data = json.loads(out.data)
                        msg = json.dumps(data, indent=2)
                    else:
                        msg = out.data
                else:
                    if out.result_type.value.lower() == "error":
                        msg = f"{out.summary}\n\n{out.cause}"
        if show and msg:
            print(msg)
        if results:
            return result, msg
        return msg
