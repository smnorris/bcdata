import json

import bcdata


with open("primary_keys.json", "r") as file:
    """validate pk database"""
    primary_keys = json.load(file)
    pk_db_tables = set(primary_keys.keys())
    bcdata_tables = set([t.lower() for t in bcdata.list_tables()])
    if pk_db_tables.issubset(bcdata_tables):
        print("Table names in primary_keys.json are valid")
        for table in primary_keys:
            column = primary_keys[table]
            schema = bcdata.get_table_definition(table)["schema"]
            if column not in [c["column_name"].lower() for c in schema]:
                print(f"Column {column} not found in {table}")
    else:
        invalid_keys = list(pk_db_tables - bcdata_tables)
        print("Invalid table names:")
        for k in invalid_keys:
            print(f" - {k}")
