"""
example usage if plan to use host and token authentication for your local workspace client and get an execution kernel
"""
from dbrxish.session import SeshBuilder
import os


session = SeshBuilder(
    host="https://example-dbrx-workspace.cloud.databricks.com",
    token=os.environ.get("MY_WORKSPACE_TOKEN"),
    cluster_id="abc012xxx"
)
spark, dbutils, wc = session.get_session()
ctx = session.execution_kernel()

# execute a code block directly on the cluster
#
# this is useful if the code you're running is interfacing with another service via the cluster's
# instance profile
print(ctx.execute("print('hello there from the cluster')"))

# use spark to run queries
spark.sql("SELECT 'just an example query running on a Databricks cluster' AS example_column").show()
