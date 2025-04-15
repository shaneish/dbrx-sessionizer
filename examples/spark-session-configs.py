"""
example usage if you need to set special spark session configs
"""
from dbrxish.session import SeshBuilder


spark, dbutils, wc = SeshBuilder().spark_config("spark.databricks.delta.autoCompact.enabled", "true").get_session()

print(f"Cluster ID: {wc.config.cluster_id}")
