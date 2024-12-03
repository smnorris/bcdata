import json
import logging
import sys

import bcdata

LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s: %(message)s"

with open("primary_keys.json", "r") as file:
    """validate pk database"""
    logging.basicConfig(stream=sys.stderr, level=20, format=LOG_FORMAT)
    log = logging.getLogger(__name__)
    primary_keys = json.load(file)
    pk_db_tables = set(primary_keys.keys())
    bcdata_tables = set([t.lower() for t in bcdata.list_tables()])
    if pk_db_tables.issubset(bcdata_tables):
        log.info("Table names in primary_keys.json are valid")
        for table in primary_keys:
            column = primary_keys[table]
            schema = bcdata.get_table_definition(table)["schema"]
            if column not in [c["column_name"].lower() for c in schema]:
                raise ValueError(f"Column {column} not found in {table}")
        log.info(
            "Validation successful - columns listed in primary_keys.json are present in listed tables"
        )
    else:
        invalid_keys = list(pk_db_tables - bcdata_tables)
        for k in invalid_keys:
            log.error(f"{k}")
        raise ValueError("Invalid table name(s)")
