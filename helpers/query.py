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
    if isinstance(key, list):
        query =  "select " + concat_keys(key, "t1")
        end = " from {table1} t1 "\
            "inner join {table2} t2 "\
            "on t1.{key} = t2.{key} "
        replacement = {"table1": table1, "table2": table2, "key": key[0]}
        end = end.format(**replacement)
        end = end + "where t1.{k} = t2.{k}".format(k=key[1]) + \
            "".join(["and t1.{k} = t2.{k} ".format(k=k) for k in key[2:]])
        query = query + end
    else:
        query = "select t1.{key} from {table1} t1 "\
                "inner join {table2} t2 on t1.{key}=t2.{key}"
        replacement = {"table1": table1, "table2": table2, "key": key}
        query = query.format(**replacement)

    return set(exec_from_string(cursor, query, quiet=True).iloc[:,0].to_list())

def get_duplicate_keys(cursor, table, key):
    if isinstance(key, list):
        query =  "select " + concat_keys(key) +\
            " from {table};".format(table=table)
    else:
        query = "select {key} from {table};"
        replacement = {"table": table, "key": key}
        query = query.format(**replacement)
    # print(query)
    keys = exec_from_string(cursor, query, quiet=True).iloc[:,0].to_list()
    ukeys = set(keys)
    return len(keys) - len(ukeys)

def get_column_matches(cs, table1, table2, key, columns):
    if isinstance(key, list):
        start =  "select t1.{k}".format(k=key[0])+ "".join(*[", t1.{k}".format(k=k) for k in key[1:]])
        end = " from {table1} t1 "\
            "inner join {table2} t2 "\
            "on t1.{key} = t2.{key} "
        replacement = {"table1": table1, "table2": table2, "key": key[0]}
        end = end.format(**replacement)
        end = end + "where t1.{k} = t2.{k}".format(k=key[1]) + "".join(["and t1.{k} = t2.{k} ".format(k=k) for k in key[2:]])
    else:
        start = "select distinct t1.{key}".format(key=key)
        end = " from {table1} t1 "\
            "inner join {table2} t2 "\
            "on t1.{key} = t2.{key}"
        replacement = {"table1": table1, "table2": table2, "key": key}
        end = end.format(**replacement)
    content = [", EQUAL_NULL(t1.{c}, t2.{c}) as {c}".format(c=c)
                for c in columns]
    query = "".join([start, *content, end])
    # print(query)
    return exec_from_string(cs, query)

def concat_keys(key, tab=None):
    if tab:
        return "concat({tab}.{k}".format(k=key[0], tab=tab) + \
            "".join([", {tab}.{k}".format(k=k, tab=tab) for k in key[1:]]) +\
            ")"
    else:
        return "concat({k}".format(k=key[0]) + \
                "".join([", {k}".format(k=k) for k in key[1:]]) +\
                ")"

# def get_column_matches_multikey(cs, table1, table2, key, columns):
#     start =  "".join(["select {k}".format(k=key[0])]+[", {k}".format(k=k) for k in key[1:]])
#     end = " from {table1} t1 "\
#         "inner join {table2} t2 "\
#         "on t1.{key} = t2.{key} "
#     replacement = {"table1": table1, "table2": table2, "key": key[0]}
#     end = end.format(**replacement)
#     end = end + "where t1.{k} = t2.{k}".format(k=key[1]) + "".join(["and t1.{k} = t2.{k} ".format(k=k) for k in key[2:]])
#     content = [", EQUAL_NULL(t1.{c}, t2.{c}) as {c}".format(c=c)
#                 for c in columns]
#     query = "".join([start, *content, end, ";"])
#     print(query)
#     return exec_from_string(cs, query)
