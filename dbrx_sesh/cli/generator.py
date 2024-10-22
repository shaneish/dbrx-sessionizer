from argparse import ArgumentParser
import datetime


import_template = """from dbrx_sesh import get_session
"""


session_template = """
spark, wc, dbutils = get_session(
{params}
)

{separator}

"""


def main():
    parser = ArgumentParser(description="Generate a Databricks script template")
    parser.add_argument(
        "--profile",
        "-p",
        type=str,
        default=None,
        help="Primary workspace client profile.",
    )
    parser.add_argument(
        "--cluster",
        "-c",
        type=str,
        default=None,
        help="Cluster name or cluster ID to use for your session.",
    )
    parser.add_argument(
        "--host",
        "-H",
        type=str,
        default=None,
        help="Host of the workspace you're connecting to.  Note that this is ignored if a --profile argument is also given.",
    )
    parser.add_argument(
        "--token_env",
        "-t",
        type=str,
        default="DATABRICKS_API_TOKEN",
        help="Environment variable name your token is stored in.  Note that this is ignored if a --profile argument is also given.",
    )
    parser.add_argument(
        "--file", "-F", type=str, default=None, help="Output file name."
    )
    parser.add_argument(
        "--file_name",
        "-f",
        type=str,
        default=None,
        help="Output file name in scratchpad folder.",
    )
    parser.add_argument(
        "--scratchpad_dir", "-s", type=str, default=".", help="Scratchpad directory."
    )
    parser.add_argument(
        "--description", "-d", type=str, default=None, help="Description of the script."
    )
    parser.add_argument(
        "--notebook_cells",
        "-n",
        action="store_true",
        help="Use the Databricks notebook cell separator.",
    )
    parser.add_argument(
        "--cell_separator",
        "-C",
        type=str,
        default="# %%",
        help="Cell separator for notebook cells.",
    )

    args = parser.parse_args()

    f_name = args.file_name
    if args.file_name is None:
        f_name_comps = ["scratch"]
        if args.profile:
            f_name_comps.append(args.profile)
        if args.cluster:
            f_name_comps.append(args.cluster)
        f_name_comps.append("{date:%Y%m%d-%H%M%S}".format(date=datetime.datetime.now()))
        f_name = "-".join(f_name_comps) + ".py"
    if args.file_name:
        file_path = args.file_name
    else:
        file_path = f"{args.scratchpad_dir}/{f_name}"
    cell_identifier = args.cell_separator
    if args.notebook_cells:
        cell_identifier = "# COMMAND ----------"
    if args.file:
        file_path = args.file
    session_args = []
    if args.profile:
        session_args.append(f"    profile = '{args.profile}',")
    if args.cluster:
        session_args.append(f"    cluster = '{args.cluster}',")
    if args.host:
        session_args.append(f"    host = '{args.host},'")
    if args.host:
        session_args.append(f"    token = os.environ['{args.profile}'],")
    session_info = "\n".join(session_args)

    with open(file_path, "w") as f:
        if args.description:
            description = args.description.replace("\n", "\n# ")
            f.write(f"# {description}\n\n")
        f.write(import_template)
        if args.token_env:
            f.write("import os\n")
        f.write(
            session_template.format(
                params=session_info,
                separator=cell_identifier,
            )
        )

    print(file_path)


if __name__ == "__main__":
    main()
