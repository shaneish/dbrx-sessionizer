"""
example usage if plan to authenticate using the default profile credentials both locally and in databricks
"""
from dbrxish.session import SeshBuilder


spark, dbutils, wc = SeshBuilder().get_session()

# use workspace client to get workspace-specific info
shanes_stuff = next(
    u for u in wc.users.list(filter="userName eq stephenson.shane.a@gmail.com")
)
print("shane's databricks user info:", shanes_stuff)

# use spark to run queries
spark.sql("SELECT 'just an example query running on a Databricks cluster' AS example_column").show()
