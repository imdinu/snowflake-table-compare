import os
import re
from termcolor import colored
from argparse import ArgumentParser

import pandas as pd

from helpers.query import snowflake_connector, exec_from_string

def escape_ansi(line):
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', line)

def dprint(*args, file=None, **kwargs):
    print(*args, **kwargs)
    if file:
        for s in args:
            file.write(escape_ansi(s) if isinstance(s, str) else escape_ansi(repr(s)))
            if "sep" in kwargs.keys():
                file.write(kwargs["sep"])
        if "end" in kwargs.keys():
            file.write(kwargs["end"])
        else: 
            file.write("\n")

if __name__ == "__main__":
    parser = ArgumentParser(description="Compare two snowflake tables")
    parser.add_argument("reference", action="store", type=str, default=None,
                    help="reference table for comparison")
    parser.add_argument("target", action="store", type=str, default=None,
                    help="target table for comparison")
    parser.add_argument("-K", "--key", action="store", type=str, default=None,
                    help="column name of unique row id (primary key)")
    parser.add_argument("-W","--warehouse", action="store", type=str, default=None,
                    help="name of the data warehouse where the tables reside")
    parser.add_argument("--credentials", action="store", type=str, 
                    default="./example.credentials.json",
                    help="file with the snowflake connection credentials")
    parser.add_argument("--outfile", action="store", type=str, default=None,
                    help="filename to redirect stdout output")
    parser.add_argument("--save-csv", action="store_true", default=False,
                    help="save table data as csv")
    parser.add_argument("--no-download", action="store_true", default=False,
                help="attempts to use previously downloaded csv files if available")

    args = parser.parse_args()

    if ";" in args.reference or ";" in args.target:
        raise ValueError("Character ';' not allowed in table names")

    ctx = snowflake_connector(path_credentials=args.credentials)
    cs = ctx.cursor()

    if args.warehouse:
        if ";" in args.warehouse:
            raise ValueError("Character ';' not allowed in warehouse name")
        cs.execute(f"USE WAREHOUSE {args.warehouse};")  

    if args.no_download:
        if os.path.exists(f"{args.reference}.csv"):
            print(f"Found existing file '{args.reference}.csv'")
            df_reference = pd.read_csv(f"{args.reference}.csv")
        else:
            query_ref = f"SELECT * FROM {args.reference}"
            df_reference = exec_from_string(cs, query_ref)

        if os.path.exists(f"{args.target}.csv"):
            print(f"Found existing file '{args.target}.csv'")
            df_target = pd.read_csv(f"{args.target}.csv")
        else:
            query_target = f"SELECT * FROM {args.target}"
            df_target = exec_from_string(cs, query_target)
    else:
        query_ref = f"SELECT * FROM {args.reference}"
        df_reference = exec_from_string(cs, query_ref)
        query_target = f"SELECT * FROM {args.target}"
        df_target = exec_from_string(cs, query_target)
    cs.close()

    f = open(f"{args.outfile}", "w") if args.outfile is not None else None
    if args.save_csv:
        df_reference.to_csv(f"{args.reference}.csv", index=False)
        df_target.to_csv(f"{args.target}.csv", index=False)
    counts_ref = [df_reference.shape[0], 
                f"{100*(df_target.shape[0]-df_reference.shape[0])/df_reference.shape[0]:.3f}%", 
                df_reference.shape[1],
                df_reference.shape[1] - df_target.shape[1]]
    counts_target= [df_target.shape[0], 
                f"{100*(df_reference.shape[0]-df_target.shape[0])/df_target.shape[0]:.3f}%", 
                df_target.shape[1],
                df_target.shape[1] - df_reference.shape[1]]
    data_counts = {args.reference: counts_ref, args.target: counts_target}
    df_counts = pd.DataFrame.from_dict(data_counts, orient='index',
                        columns=['N Rows', 'Rows %Diff', 'N Columns', 'Cols Diff'])
    dprint("\n\t\t", colored("DATA COUNTS", "green"), "\t\t", file=f)
    dprint(df_counts, file=f)

    dprint("\n\t\t", colored("COLUMN COMPOSITION", "green"), "\t\t", file=f)
    dprint(colored("Common columns:", "cyan"), end=" ", file=f)
    common_columns = set(df_target.columns).intersection(set(df_reference.columns))
    dprint(*common_columns, sep=", ", file=f)
    unique_ref = set(df_target.columns) - set(df_reference.columns)
    unique_target = set(df_reference.columns) - set(df_target.columns)
    if unique_ref:
        dprint(colored(f"Columns unique to {args.reference}:", "cyan"), end=" ", file=f)
        dprint(*unique_ref, sep=", ", file=f)
    else:
         dprint(colored(f"No columns unique to {args.reference}.", "cyan"), file=f)
    if unique_target:
        dprint(colored(f"Columns unique to {args.target}:", "cyan"), end=" ", file=f)
        dprint(*unique_target, sep=", ", file=f)
    else:
         dprint(colored(f"No columns unique to {args.target}.", "cyan"), file=f)

    if args.key:
        args.key = str.upper(args.key)
        if args.key not in common_columns:
            raise ValueError("Key is not in common columns")

        common_rows = set(df_reference[args.key].values.flatten().tolist())\
            .intersection(set(df_target[args.key].values.flatten().tolist()))

        dprint("\n\t\t", colored("ROW MATCHING", "green"), "\t\t", file=f)
        dprint("Matching by ID: ", colored(f"{args.key}", "red"), file=f)
        dprint(colored(f"Common rows count {args.reference}\t", "cyan"),
                f"{len(common_rows)}/{df_reference.shape[0]}\t",
                f"{100*len(common_rows)/df_reference.shape[0]:.3f}%", file=f)
        dprint(colored(f"Common rows count {args.target}\t", "cyan"),
                f"{len(common_rows)}/{df_target.shape[0]}\t",
                f"{100*len(common_rows)/df_target.shape[0]:.3f}%", file=f)

        df_reference = df_reference[df_reference[args.key].isin(common_rows)].fillna(0)
        df_target = df_target[df_target[args.key].isin(common_rows)].fillna(0)

        df_reference = df_reference.sort_values(by=args.key).reset_index(drop=True)
        df_target = df_target.sort_values(by=args.key).reset_index(drop=True)

        comparison = df_reference.compare(df_target, keep_shape=True)
        comparison = comparison.T.xs("self", level=1).T

        results = pd.DataFrame(comparison.isnull().sum(axis=0)).T * 100 / df_reference.shape[0]
        results = results.T.reset_index()
        results.columns = ["Column", "% value match"]

        dprint("\n\t\t", colored("INDIVIDUAL VALUE MATCHING", "green"), "\t\t", file=f)
        dprint(results, file=f)

        compound_score = (len(common_rows)/df_counts.iloc[1,0])*\
                         (len(common_columns)/len(df_target.columns))*\
                         results["% value match"].mean()
        dprint("\n\t\t", colored("COMPOUND MATCH SCORE: ", "green"), 
                colored(f"{compound_score:.2f}%", "red"), "\t\t", file=f)

    if args.outfile:
        f.close()


