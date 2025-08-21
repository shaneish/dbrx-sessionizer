# databricks sessionizer

a simple library that includes various databricks workspace admin and user processes.  primarily used for interacting with databricks workspaces and writing admin code for databricks workspaces that run the same locally as in a databricks notebook or script.  additionally, it includes a cli command for running sql queries against databricks sql warehouses and metastore.

this library currently only support 15.4 lts runtimes on python3.11+.  if you want older runtimes, feel free to make an issue and submit a pr.

## what it do
a number of things that makes it easier to develop databricks code locally.  i primarily use this to help write scripts and notebooks for databricks workspace administration, but it can be used for other things as well.  specifically, this is setup to make it easy to write code which can be run as-is locally and deployed as a databricks job.  the main features are:
1) generate sessions (spark, dbutils, and workspace clients) with optional configurations depending on where code is being run so that code will run in any environment -- local repl, databricks script, or databricks notebook.
2) execute arbitrary code on remote databricks clusters (this is helpful if developing code that relies on a specific cluster config, such as instance profile)
3) sql editor: write and run sql statements on remove sql warehouses from anywhere
  - your query history is retained so you can reference previous queries and their output
  - returns a QueryResult object to make remote use easier:
    * query output display formatted to make it easier to read and examine in a terminal
    * easily save query output to csv or json
    * convert query output to a dataframe for further investigation if needed (only use for exploration purposes, obviously create a proper dataframe using `spark.table()` or `spark.sql()` methods -- building a big dataframe from a list of rows via api is not efficient or reliable)
4) metastore sql queries from the command line (see [cli](#cli) section below)

## installation
clone this repo onto your local machine, cd into the repo root, and run:
```python
python3 -m pip install .
```

can also pip install from the git repo by doing:
```python
python3 -m pip install git+https://github.com/shaneish/dbrx-sessionizer.git
```
## usage
usage examples can be found in the [examples](./examples) folder.

## cli
there's also an associated cli command for running sql queries.  when you `pip install` the project, the cli command can be used via `dxsh` (DatabriX seSH -- ya, i know, it sucks).

examples:
```bash
# query table and output csv to a file
dxsh --sql "select * from some_catalog.some_schema.some_table" -o csv > /tmp/output.csv

# query table and output a formatted table to terminal
dxsh --sql "select * from some_catalog.some_schema.some_table"

# query table using a different auth profile and output json to terminal
dxsh --sql "select * from some_catalog.some_schema.some_table" -o json -p TEST-PROFILE

# query table with a different warehouse and output formatted table to terminal
dxsh --sql "select * from some_catalog.some_schema.some_table" --warehouse_id 999fff3fd3d78f9d # can also use -W instead
```


# todo
1) refactor ExecutionKernel code
  - abstract out cluster management into separate class
  - structure execution output better
  - clean it up.  it's a mess, easily the worst part of this code.  was basically hacked together just to get something working quickly with zero cleanup afterwards
2) sql query repl
3) remote execution repl
