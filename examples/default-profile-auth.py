"""
example usage if plan to authenticate using the default profile credentials both locally and in databricks
"""

from dbrxish.session import SeshBuilder


wc, spark, dbutils = SeshBuilder().sesh()

user_shane = next(
    u for u in wc.users.list(filter=f"userName eq stephenson.shane.a@gmail.com")
)
