import tomllib
from pathlib import Path
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import SparkSession
from dataclasses import dataclass
import re
import os


@dataclass
class DummyCluster:
    cluster_id: str | None = None


def dbrx_cfg(cfg_path: str) -> dict[str, dict[str, str]]:
    if Path(cfg_path).exists():
        with open(cfg_path, "r") as f:
            config = f.read()
            config = re.sub(r"(\w+\s*=\s*)(.*)", r'\1"\2"', config)
            return tomllib.loads(config)
    return dict()


def get_session(
    profile: str | None = None,
    cluster: str | None = None,
    cfg_file: str | None = None,
) -> tuple[SparkSession | None, WorkspaceClient, RemoteDbUtils]:
    cfg_path = (
        cfg_file
        or os.environ.get("DATABRICKS_CONFIG_FILE")
        or str(Path.home() / ".databrickscfg")
    )
    cfg = dbrx_cfg(cfg_path)
    profile = profile or "DEFAULT"
    wc = WorkspaceClient(profile=profile, config_file=cfg_path)
    dbutils = wc.dbutils
    spark, cluster_id = None, None
    cluster = cluster or cfg.get(profile, {}).get("cluster")
    if cluster:
        matching_cluster = DummyCluster()
        try:
            matching_cluster = wc.clusters.get(cluster)
        except:
            try:
                matching_cluster = next(
                    c for c in wc.clusters.list() if c.cluster_name == cluster
                )
            except:
                print(
                    f"[info] Unable to identify cluster {cluster} by `cluster_id` or `cluster_name`"
                )
        cluster_id = matching_cluster.cluster_id
    if cluster_id:
        config = Config(profile=profile, cluster_id=cluster_id)
        spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    return spark, wc, dbutils
