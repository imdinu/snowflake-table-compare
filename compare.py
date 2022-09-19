import os
import re
from termcolor import colored
from argparse import ArgumentParser

import pandas as pd

from helpers.snowflake import snowflake_connector, exec_from_string
from helpers.query import * 

def escape_ansi(line):
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', line)

def dprint(*args, file=None, **kwargs):
    print(*args, **kwargs)
    if file:
        for s in args:
            file.write(escape_ansi(s) if isinstance(s, str) else escape_ansi(repr(s)))
            if "sep" in kwargs:
                file.write(kwargs["sep"])
        if "end" in kwargs:
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
                    default="./default.credentials.json",
                    help="file with the snowflake connection credentials")
    parser.add_argument("--outfile", action="store", type=str, default=None,
                    help="filename to redirect stdout output")
    parser.add_argument("--save-csv", action="store_true", default=False,
                    help="save detailed matching data as csv")

    args = parser.parse_args()

    if ";" in args.reference or ";" in args.target:
        raise ValueError("Character ';' not allowed in table names")

    # ctx = snowflake_connector(path_credentials="./.credentials.json")
    ctx = snowflake_connector(path_credentials=args.credentials)
    cs = ctx.cursor()

    if args.warehouse:
        if ";" in args.warehouse:
            raise ValueError("Character ';' not allowed in warehouse name")
        cs.execute(f"use warehouse {args.warehouse};")  

    f = open(f"{args.outfile}", "w") if args.outfile is not None else None

    c1 = get_table_columns(cs, args.reference)
    c2 = get_table_columns(cs, args.target)
    r1, r2 = get_row_numbers(cs, args.reference, args.target)

    c_inter = c1.intersection(c2)
    c_union = c1.union(c2)
    dprint("\n\t\t", colored("DATA COUNTS", "green"), "\t\t", file=f)

    dprint(colored(f"{len(c_inter)}/{len(c1)}", "cyan"), 
            "column names common between tables", file=f)
    # dprint("Found columns:", *[colored(f"{c}", "green") 
    #             for c in c_inter], file=f)
    dprint("Missing columns:", *[colored(f"{c}", "red") 
                    for c in (c1-c_inter)], file=f)
    dprint(f"{args.reference} row count: ", colored(f"{r1}", "cyan"), file=f)
    dprint(f"{args.target} row count: ", colored(f"{r2}", "cyan"), file=f)
    dprint("Row counts difference: ", colored(f"{abs(r1-r2)}", "red"), file=f)

    dprint("\n\t\t", colored("COLUMN MATCH", "green"), "\t\t", file=f)

    ids_match = get_table_keys(cs, args.reference, args.target, args.key)
    cols = c_inter - {args.key.upper()}
    df = get_column_matches(cs, args.reference, args.target, args.key, cols)
    cols_matches = df[list(cols)].sum(axis=0) *100/df.shape[0]
    cols_df = pd.DataFrame(cols_matches.sort_values(ascending=False))
    cols_df = cols_df.rename(columns={0: "Match %"})

    ndupes = get_duplicate_keys(cs, args.target, args.key)
    dprint(colored(f"{len(ids_match)}/{r1}", "green"), " matches on ", 
        colored(f"{args.key.upper()} ", "cyan"),
        colored(f"= {100*len(ids_match)/r1:.2f} %", "green"), file=f)
    if ndupes > 0:
        dprint(colored(f"{ndupes}/{r2}", "red"), " duplicates on ", 
            colored(f"{args.key.upper()} ", "cyan"),
            colored(f"= {100*ndupes/r2:.2f} %", "red"), file=f)
    else:
        dprint(colored("No duplicates ", "green"), "of ", colored(f"{args.key.upper()} ", "cyan"))
    dprint(cols_df.to_string(), file=f)
    dprint("\n", colored(f"{cols_matches.mean():.3f} %", "green"), 
            " overall match", file=f)

    if args.save_csv:
        filename = f"{args.target.split('.')[-1]}.csv"
        df.to_csv(filename, sep=";")

    # dprint(df_counts, file=f)

    # dprint("\n\t\t", colored("COLUMN COMPOSITION", "green"), "\t\t", file=f)
    # dprint(colored("Common columns:", "cyan"), end=" ", file=f)
    # cs.close()

    # schema1 = "pc_alooma_db.analytics"
    # schema2 = "intedasol.analytics"

    # out1 = get_schema_tables(cs, schema1)
    # out2 = get_schema_tables(cs, schema2)

    # inter = out1.intersection(out2)
    # union = out1.union(out2)

    # print(f"{len(inter)}/{len(union)} tables found in both schemas")

    # table1 = "pc_alooma_db.analytics.account"
    # table2 = "intedasol.analytics.account"

    # c1 = get_table_columns(cs, table1)
    # c2 = get_table_columns(cs, table2)
    # r1, r2 = get_row_numbers(cs, table1, table2)

    # c_inter = c1.intersection(c2)
    # c_union = c1.union(c2)
    # print(f"{len(c_inter)}/{len(c_union)} column names common between tables")
    # print("Missing columns:", *(c_union-c_inter))
    # print(f"{table1} row count: {r1}")
    # print(f"{table2} row count: {r2}")
    # print(f"row counts difference: {abs(r1-r2)}")

    # primary_key = "SFDCID"

    # ids_match = get_table_keys(cs, table1, table2, primary_key)

    # print(f"{len(ids_match)}/{r1} matches on {primary_key.upper()} "
    #     f"= {100*len(ids_match)/r1:.2f} %")

    # cols = c_inter - {primary_key}

    # df = get_column_matches(cs, table1, table2, primary_key, cols)

    # print(100/len(ids_match))
    # cols_matches = df[list(cols)].sum(axis=0) *100/len(ids_match)
    # print(cols_matches.sort_values(ascending=False))
    # print(cols_matches.mean())

