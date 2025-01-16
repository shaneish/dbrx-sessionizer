from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language, ClusterDetails, Wait, Results
from datetime import timedelta, datetime
import json


def get_language(language: str) -> Language:
    if language is not None:
        match language.lower():
            case "python":
                return Language.PYTHON
            case "scala":
                return Language.SCALA
            case "sql":
                return Language.SQL
            case _:
                return Language.PYTHON


class ExecutionKernel:
    def __init__(
        self,
        wc: WorkspaceClient,
        cluster_id: str | None = None,
        verbose: bool = False,
        default_language: str = Language.PYTHON,
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
    def cluster_ready(self) -> None | ClusterDetails:
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
            msg = f"Session is ready"
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
