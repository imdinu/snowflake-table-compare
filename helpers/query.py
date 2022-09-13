import pandas as pd

from .snowflake import exec_from_string


def get_schema_tables(cursor, schema_name):
    query = "select TABLE_NAME from {database}.INFORMATION_SCHEMA.TABLES "\
            "where TABLE_SCHEMA = '{schema}'"
    database, schema = schema_name.upper().split(".")
    replacement = {"database": database, "schema": schema}
    query = query.format(**replacement)

    return set(exec_from_string(cursor, query).iloc[:,0].to_list())

def get_table_columns(cursor, table_name):
    query = "select COLUMN_NAME from {database}.INFORMATION_SCHEMA.COLUMNS "\
            "where TABLE_SCHEMA = '{schema}' and TABLE_NAME = '{table}'"
    database, schema, table = table_name.upper().split(".")
    replacement = {"database": database, "schema": schema, "table": table}
    query = query.format(**replacement)

    return set(exec_from_string(cursor, query).iloc[:,0].to_list())

def get_row_numbers(cursor, table1, table2):
    query = "select 1 as id, count(*) as c "\
            "from {table1} "\
            "union (select 2 as id, count(*) as c "\
                "from {table2})"
    replacement = {"table1": table1, "table2": table2}
    query = query.format(**replacement)
    df = exec_from_string(cursor, query)

    return int(df.iloc[0,1]), int(df.iloc[1,1])

def get_table_keys(cursor, table1, table2, key):
    query = "select t1.{key} from {table1} t1 "\
            "inner join {table1} t2 on t1.{key}=t2.{key}"
    replacement = {"table1": table1, "table2": table2, "key": key}
    query = query.format(**replacement)

    return set(exec_from_string(cursor, query, quiet=True).iloc[:,0].to_list())

def get_column_matches(cs, table1, table2, key, columns):
    start = "select t1.{key}".format(key=key)
    end = " from {table1} t1 "\
        "inner join {table2} t2 "\
        "on t1.{key} = t2.{key};"
    replacement = {"table1": table1, "table2": table2, "key": key}
    end = end.format(**replacement)
    content = [", EQUAL_NULL(t1.{c}, t2.{c}) as {c}".format(c=c)
                for c in columns]
    query = "".join([start, *content, end])
    # print(query)
    return exec_from_string(cs, query)
