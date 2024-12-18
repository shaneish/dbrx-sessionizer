# in this example, we'll show how to create a databricks connect session when you've
# added the cluster you want to use in the session as an additional field in the
# `~/.databrickscfg` profile you plan to use ('dev' in this example here).
#
# assume you have the following section in your ~/.databrickscfg
# this will then load that profile and the appropriate cluster into your databricks session
"""
[dev]
host  = https://example-dev.cloud.databricks.com/
token = dapiexampletoken
cluster_id = 0123-456789-ex12345
"""

from dbrx_sesh.session import get_session


spark, wc, dbutils = get_session(profile="dev")

# %%

user_shane = next(
    u for u in wc.users.list(filter=f"userName eq stephenson.shane.a@gmail.com")
)
