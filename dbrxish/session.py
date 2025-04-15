from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from dbrxish.execution import ExecutionKernel, SQLEditor
from typing import Self
from os import environ


class SeshBuilder:

    def __init__(self, *args, **kwargs):
        self._default_config: tuple[tuple, dict[str, str]] | None = None
        if args or kwargs:
            self._default_config = (args, kwargs)
        self._local_config: tuple[tuple, dict[str, str]] | None = None
        self._remote_config: tuple[tuple, dict[str, str]] | None = None
        self._workspace_client: WorkspaceClient | None = None
        self._spark_configs: dict | None = None
        self._is_remote = environ.get("DATABRICKS_RUNTIME_VERSION") is not None
        self._app_name: str | None = None
        self._spark: SparkSession | None = None
        self._workspace_client: WorkspaceClient | None = None
        self._dbutils: DBUtils | RemoteDbUtils | None = None

    def spark_configs(self, **kwargs: dict[str, str]) -> Self:
        self._spark_configs = kwargs
        return self

    def app_name(self, name: str) -> Self:
        self._app_name = name
        return self

    def remote_configs(self, *args, **kwargs) -> Self:
        if args or kwargs:
            self._remote_config = (args, kwargs)
        return self

    def local_configs(self, *args, **kwargs) -> Self:
        if args or kwargs:
            self._local_config = (args, kwargs)
        return self

    @property
    def spark_client_enabled(self) -> bool:
        return self.spark.conf.get("spark.databricks.service.client.enabled") == "true"

    @property
    def config(self) -> Config:
        if self._is_remote:
            return self._remote_config or self._default_config
        return self._local_config or self._default_config

    @property
    def spark(self) -> SparkSession:
        if not self._spark:
            if not self._is_remote:
                from databricks.connect import DatabricksSession

                spark = DatabricksSession.builder.sdkConfig(self.config)
            else:
                spark = SparkSession.builder
            if self._app_name:
                spark = spark.appName(self._app_name)
            if self._spark_configs:
                spark = spark.config(map=self._spark_configs)
            self._spark = spark.getOrCreate()
        return self._spark

    @property
    def workspace_client(self) -> WorkspaceClient:
        self._workspace_client = self._workspace_client or WorkspaceClient(config=self.config)
        return self._workspace_client

    @property
    def dbutils(self) -> DBUtils | RemoteDbUtils:
        if self.spark_client_enabled:
            self._dbutils = DBUtils(self.spark)
        else:
            self._dbutils = self.workspace_client.dbutils
        return self._dbutils

    # just an alias for what I like to use
    @property
    def wc(self) -> WorkspaceClient:
        return self.workspace_client

    def get_session(self) -> tuple[SparkSession, DBUtils | RemoteDbUtils, WorkspaceClient]:
        return self.spark, self.dbutils, self.workspace_client

    def sql_editor(self) -> SQLEditor:
        return SQLEditor(wc=self.wc, warehouse_id=self.config.warehouse_id)

    def execution_kernel(self, **kwargs) -> ExecutionKernel:
        return ExecutionKernel(wc=self.wc, cluster_id=self.config.cluster_id, **kwargs)
