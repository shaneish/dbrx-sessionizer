# in this example, we'll show how to create a databricks connect session with your
# workspace host and personal access token.  In this case, we've hardcoded the host URL and are reading
# our token in from the shell variables as "DATABRICKS_PERSONAL_ACCESS_TOKEN"

from dbrx_sesh.session import get_session
import os


spark, wc, dbutils = get_session(
    cluster="0123-456789-ab9cdd69",
    host="https://example-workspace-dev.cloud.databricks.com",
    token=os.environ["DATABRICKS_PERSONAL_ACCESS_TOKEN"],
)

# %%

user_shane = next(
    u for u in wc.users.list(filter=f"userName eq stephenson.shane.a@gmail.com")
)
