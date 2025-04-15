"""
example usage for local repl sql editor
"""
from dbrxish.session import SeshBuilder


session = SeshBuilder(profile="DEFAULT-PROFILE")
sql = session.sql_editor()

query_output = sql("""
    select * from cool_people
    where user = 'stephenson.shane.a@gmail.com'
""")

if query_output is not None: # this is just done to fix lsp complaints, the above query is guaranteed to return something
    # show structured preview of query result
    print(query_output)

    # save query as csv
    query_output.csv("query_output.csv")

    # want to do more?  convert to df (this is slow and inefficient, so you should
    # simply create the dataframe the right way with `spark.sql()` when using in
    # production.  this was just something quick i threw together to use when
    # exploring data.
    df = query_output.to_df(session.spark)
