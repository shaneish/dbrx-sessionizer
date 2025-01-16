from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from typing import Any, Self
from os import environ
from enum import Enum

DBUType = DBUtils | RemoteDbUtils


class Env(Enum):
    LOCAL = 0
    DATABRICKS = 1
    BOTH = 2

    def __eq__(self, other: Self) -> bool:
        return (self.value == other.value) or (self.value == 2) or (other.value == 2)


class SeshBuilder:
    _RUNTIME_ENV_CHECK = ("DATABRICKS_RUNTIME_VERSION", None)

    def __init__(
        self,
        sesh_config: None | dict[str, Any] = None,
        wksp_cfg: None | dict[str, Any] = None,
        use_config_if_missing: bool = True,
    ):
        self.config = None
        self.wksp_client = None
        self.dbutils = None
        self.spark = None
        self.use_config_if_missing = use_config_if_missing
        self.sesh_config = sesh_config or {}
        self.wksp_cfg = wksp_cfg or self.wksp_cfg_backup
        self._current_env = self._get_current_env()

    def _get_current_env(self) -> Env:
        if environ.get(self._RUNTIME_ENV_CHECK[0]) == self._RUNTIME_ENV_CHECK[1]:
            return Env.DATABRICKS
        else:
            return Env.LOCAL

    @property
    def wksp_cfg_backup(self) -> dict[str, Any]:
        return (
            {"config": self.config}
            if (self.use_config_if_missing and self.config)
            else {}
        )

    def set_config(self, env: Env = Env.BOTH, **sesh_config: Any) -> Self:
        if sesh_config and env == self._current_env:
            self.sesh_config = sesh_config
        self.config = Config(**self.sesh_config)
        return self

    def set_wksp(self, env: Env = Env.BOTH, **wksp_cfg: Any) -> Self:
        if wksp_cfg and (env == self._current_env):
            self.wksp_cfg = wksp_cfg
        return self

    def _get_wksp(self, **wksp_cfg: Any) -> Self:
        self.wksp_client = WorkspaceClient(**(wksp_cfg or self.wksp_cfg))
        return self

    def _get_spark(self) -> Self:
        if globals().get("spark"):
            self.spark = globals().get("spark")
        if self._current_env == Env.DATABRICKS:
            from databricks.sdk.runtime import spark

            self.spark = spark
        else:
            from databricks.connect import DatabricksSession

            if not self.config:
                self.set_config()
            self.spark = DatabricksSession.builder.sdkConfig(self.config).getOrCreate()
        return self

    def _get_dbutils(self) -> Self:
        if globals().get("dbutils"):
            self.dbutils = globals().get("dbutils")
        if self.spark:
            if self.spark.conf.get("spark.databricks.service.client.enabled") == "true":
                self.dbutils = DBUtils(self.spark)
        if self.wksp_client:
            self.dbutils = self.wksp_client.dbutils
        else:
            from databricks.sdk.runtime import dbutils

            self.dbutils = dbutils
        return self

    def build(self) -> Self:
        self.set_config()
        self.set_wksp()
        self._get_wksp()
        self._get_spark()
        self._get_dbutils()
        return self

    def sesh(self) -> tuple[WorkspaceClient, SparkSession, DBUType]:
        self.build()
        return self.wksp_client, self.spark, self.dbutils
