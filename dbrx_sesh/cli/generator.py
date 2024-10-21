from argparse import ArgumentParser
import datetime


template = """from dbrx_sesh import get_session


spark, wc, dbutils = get_session(
    profile = {profile},
    cluster = {cluster},
)

{separator}

"""


def quotate(s: str | None) -> str:
    if s:
        return f'"{s}"'
    else:
        return "None"


def main():
    parser = ArgumentParser(description="Generate a Databricks script template")
    parser.add_argument(
        "--profile",
        "-p",
        type=str,
        default=None,
        help="Primary workspace client profile.",
    )
    parser.add_argument("--cluster", "-c", type=str, default=None)
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

    with open(file_path, "w") as f:
        if args.description:
            description = args.description.replace("\n", "\n# ")
            f.write(f"# {description}\n\n")
        f.write(
            template.format(
                profile=quotate(args.profile),
                cluster=quotate(args.cluster),
                separator=cell_identifier,
            )
        )

    print(file_path)


if __name__ == "__main__":
    main()
