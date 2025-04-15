"""
example usage if you need to use a specific `~/.databrickscfg` profile when run locally but want to use default user workspace credentials and/or cluster when run in notebook
"""
from dbrxish.session import SeshBuilder


spark, dbutils, wc = SeshBuilder().local_configs(profile="LOCAL_PROFILE").get_session()

print(f"Cluster ID: {wc.config.cluster_id}")
