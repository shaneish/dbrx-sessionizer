"""
example usage if plan to use host and token authentication for your local workspace client and get an execution kernel
"""

from dbrxish.session import SeshBuilder, Env
from dbrxish.execution import ExecutionKernel
import os


wc, spark, dbutils = (
    SeshBuilder()
    .set_wksp(
        Env.LOCAL,
        host="https://example-dbrx-workspace.cloud.databricks.com",
        token=os.environ.get("MY_WORKSPACE_TOKEN"),
    )
    .sesh()
)
ctx = ExecutionKernel(wc)

print(ctx.execute("print('hello there from the cluster')"))
