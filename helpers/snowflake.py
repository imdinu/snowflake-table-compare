import json
from warnings import warn
from getpass import getpass
from os.path import exists
from pathlib import Path

import pandas as pd
from snowflake import connector
from tqdm import tqdm


def snowflake_connector(
    path_credentials="default.credentials.json"
):
    path_credentials = Path(path_credentials)
    if exists(path_credentials):
        with open(path_credentials, "r") as f:
            credentials = json.load(f)
        account = credentials["account"]
        user = credentials["user"]
        pwd = credentials["pass"]
    else:
        if path_credentials is not None:
            warn(f"{path_credentials} not found." "Switching to manual login")
        user = input("Snowflake Username")
        pwd = getpass("Snowflake Password")

    return connector.connect(user=user, password=pwd, account=account)


def fetch_chunks(cursor, quiet=False):
    total = len(cursor.get_result_batches())
    if total <= 1:
        df = cursor.fetch_pandas_all()
        if df.shape[0] == 0:
            print("Querry produced no results!")
        return df
    dfs = list(
        tqdm(
            cursor.fetch_pandas_batches(),
            total=total,
            desc="Downloading chunks",
            unit="batch",
            disable=quiet
        )
    )
    return pd.concat(dfs).reset_index(drop=True)


def exec_from_file(cursor, path_sql, delim=";", quiet=False):
    sql_queries = Path(path_sql).read_text()
    sql_queries = [q + delim for q in sql_queries.split(delim) if q]
    dfs = [fetch_chunks(cursor.execute(q), quiet=quiet) for q in sql_queries]
    return dfs if len(dfs) > 1 else dfs[0]

def exec_from_string(cursor, query_string, delim=";", quiet=False):
    sql_queries = [q + delim for q in query_string.split(delim) if q]
    dfs = [fetch_chunks(cursor.execute(q), quiet=quiet) for q in sql_queries]
    return dfs if len(dfs) > 1 else dfs[0]


