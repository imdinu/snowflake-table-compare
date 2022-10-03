#!/usr/bin/env python

from argparse import ArgumentParser

import pandas as pd

from helpers.snowflake import snowflake_connector, exec_from_string
from helpers.query import *
from compare import compare_tables, dprint


if __name__ == "__main__":
    parser = ArgumentParser(description="Compare two snowflake tables")
    parser.add_argument("reference", action="store", type=str, default=None,
                    help="csv files for bulk comparison")
    parser.add_argument("-W","--warehouse", action="store", type=str, default=None,
                    help="name of the data warehouse where the tables reside")
    parser.add_argument("--credentials", action="store", type=str, 
                    default="./default.credentials.json",
                    help="file with the snowflake connection credentials")
    parser.add_argument("--nologs", action="store_true", default=False,
                    help="suppress log file writing")
    parser.add_argument("--save-csv", action="store_true", default=False,
                    help="save detailed matching data as csv")

    args = parser.parse_args()

    # ctx = snowflake_connector(path_credentials="./.credentials.json")
    ctx = snowflake_connector(path_credentials=args.credentials)
    cs = ctx.cursor()

    if args.warehouse:
        if ";" in args.warehouse:
            raise ValueError("Character ';' not allowed in warehouse name")
        cs.execute(f"use warehouse {args.warehouse};")  

    # f = None if args.nologs else open(f"{args.outfile}", "w")
    ref_df = pd.read_csv(args.reference, skiprows=1).iloc[:,[0,1,2,4,5,6,7]]
    ref_df = ref_df[ref_df.iloc[:,-1].str.isalnum().fillna(False)]

    refs = ref_df.iloc[:,0] + "." + ref_df.iloc[:,1] + "." + ref_df.iloc[:,2]
    targs = ref_df.iloc[:,3] + "." + ref_df.iloc[:,4] + "." + ref_df.iloc[:,5]
    tables = pd.DataFrame([refs, targs, ref_df.iloc[:,-1]]).T.reset_index(drop=True)
    tables = tables.rename(columns={"Unnamed 0":"Reference", 
                                "Unnamed 1":"Target", 
                                "Unnamed: 7":"Key"})
    
    print(f"\t\t FOUND {tables.shape[0]} TABLES TO COMPARE:")
    print(tables.to_string(), "\n", "\n")

    for row in tables.iterrows():
        f = None if args.nologs else open(f"{row[1].Reference.split('.')[-1]}.log", "w")
        print(f"\tComparing {row[1].Reference} vs {row[1].Target} based on {row[1].Key}")
        compare_tables(cs, row[1].Reference, row[1].Target, row[1].Key, f, args.save_csv)

    # compare_tables(cs, args.reference, args.target, args.key, f, args.save_csv)