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
import sys


def dbrx_cfg_extras(cfg_path: str) -> dict[str, str]:
    if Path(cfg_path).exists():
        with open(cfg_path, "r") as f:
            config = f.read()
            config = re.sub(r'(\w+\s*=\s*)(.*)', r'\1"\2"', config)
            return {k: v for k, v in tomllib.loads(config).items() if "token" not in k}
    return dict()


def parse_args(cfg_path: str, prioritize_cfg: bool = False) -> dict[str, str | None]:
    args_map = {
        "profile": (["--profile", "-p"], "DATABRICKS_SCRIPT_PROFILE"),
        "cluster": (["--cluster", "-c"], "DATABRICKS_SCRIPT_CLUSTER"),
        "env": (["--env", "-e"], "DATABRICKS_SCRIPT_ENV"),
        "catalog": (["--catalog", "-C"], "DATABRICKS_SCRIPT_PRIMARY_CATALOG")
    }
    d = dbrx_cfg_extras(cfg_path)
    for var, specs in args_map.items():
        flags, env_var = specs[:2]
        val = os.environ.get(env_var)
        for flag in flags:
            if flag in sys.argv:
                try:
                    val = sys.argv[sys.argv.index(flag) + 1]
                except:
                    print(f"[error] No value for `{var}` found after {flag} flag.")
        if prioritize_cfg:
            d[var] = d.get(var) or val
        else:
            d[var] = val or d.get(var)
    return d


@dataclass
class DummyCluster:
    cluster_id: str | None = None


def get_session(
    profile: str | None = None,
    cluster: str | None = None,
    cfg_file: str | None = None,
    prioritize_cfg: bool = False,
    **default_args,
) -> tuple[SparkSession | None, WorkspaceClient, RemoteDbUtils, dict[str, str | None]]:
    cfg_path = cfg_file or os.environ.get("DATABRICKS_CONFIG_FILE") or str(Path.home() / ".databrickscfg")
    args = parse_args(cfg_path, prioritize_cfg)
    if default_args:
        for k, v in default_args.items():
            args[k] = args.get(k) or v
    wc = WorkspaceClient(profile=args.get("profile") or "DEFAULT", config_file=cfg_file)
    dbutils = wc.dbutils
    config, spark = None, None
    cluster = cluster or args.get("cluster")
    if cluster:
        matching_cluster = DummyCluster()
        try:
            matching_cluster = wc.clusters.get(cluster)
        except:
            try:
                matching_cluster = next(c for c in wc.clusters.list() if c.cluster_name == cluster)
            except:
                print(f"[info] Unable to identify cluster {cluster} by `cluster_id` or `cluster_name`")
        cluster_id = matching_cluster.cluster_id
        if cluster_id:
            config = Config(profile=profile, cluster_id=cluster_id)
            spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    return spark, wc, dbutils, args

