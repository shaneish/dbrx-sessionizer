"""
example usage if plan to authenticate using a non-default profile locally
"""

from dbrxish.session import SeshBuilder, Env
from dbrxish.execution import get_language, ExecutionKernel


wc, spark, dbutils = (
    SeshBuilder().set_config(Env.LOCAL, profile="dev-admin-medium").sesh()
)

user_shane = next(
    u for u in wc.users.list(filter=f"userName eq stephenson.shane.a@gmail.com")
)
