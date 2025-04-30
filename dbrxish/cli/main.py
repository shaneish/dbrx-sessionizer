from dbrxish import session
from importlib import metadata
from argparse import ArgumentParser

CONFIG_PARAMS = ["warehouse_id", "cluster_id", "profile", "host", "username", "password"]

def sql_query(session: session.SeshBuilder, query: str, output_file: str | None = None, output_format: str | None = None, preview_limit: int | None = None):
    sql = session.sql_editor()
    table = sql(query)
    if table:
        table.preview = preview_limit
        match output_format:
            case "json":
                table.write_json(output_file)
            case "csv":
                table.write_csv(output_file)
            case _:
                table.write_display(output_file)
        return table


def parse_args():
    parser = ArgumentParser(
        prog="dxs",
        description="Databricks sessions from the command line."
    )
    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="Show current version of fnkpy",
    )
    parser.add_argument(
        "--profile",
        "-p",
        type=str,
        help="Databricks profile for authentication."
    )
    parser.add_argument(
        "--warehouse-id",
        "-W",
        type=str,
        help="Warehouse ID to use."
    )
    parser.add_argument(
        "--cluster-id",
        "-C",
        type=str,
        help="Cluster ID to use."
    )
    parser.add_argument(
        "--sql",
        "-s",
        type=str,
        help="SQL Query to run."
    )
    parser.add_argument(
        "--host",
        "-H",
        type=str,
        help="Databricks host for authentication."
    )
    parser.add_argument(
        "--username",
        "-U",
        type=str,
        help="Databricks username for authentication."
    )
    parser.add_argument(
        "--password",
        "-P",
        type=str,
        help="Databricks password for authentication."
    )
    parser.add_argument(
        "--account_id",
        "-A",
        type=str,
        help="Databricks account ID for authentication."
    )
    parser.add_argument(
        "--output-file",
        "-of",
        type=str,
        help="Name of file to write output to."
    )
    parser.add_argument(
        "--output-format",
        "-o",
        type=str,
        default="display",
        help="Output format for SQL query.  Should be `json`, `csv`, or `display`."
    )
    parser.add_argument(
        "--limit-output",
        "-l",
        type=int,
        help="Preview limit for show SQL output"
    )
    args = parser.parse_args()
    args.config_params = {key: val for key, val in vars(args).items() if key in CONFIG_PARAMS}
    return args


def main():
    args = parse_args()
    sesh = session.SeshBuilder(**args.config_params).set_fast_dbutils()

    if args.version:
        print(f"dxsh {metadata.version('dbrxish')}")
    elif args.sql:
        sql_query(sesh, args.sql, args.output_file, args.output_format)


if __name__ == "__main__":
    main()
