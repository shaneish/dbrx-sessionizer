# in this example, assume you have the following section in your ~/.databrickscfg
# this will then load that profile and the appropriate cluster into your databricks session
"""
[dev]
host  = https://example-dev.cloud.databricks.com/
token = dapiexampletoken
cluster = 0123-456789-ex12345
"""
from dbrx_sesh.session import get_session


spark, wc, dbutils, args = get_session(
    profile = "dev",
    cluster = None,
)

# %%

user_shane = next(u for u in wc.users.list(filter=f"userName eq stephenson.shane.a@gmail.com")
