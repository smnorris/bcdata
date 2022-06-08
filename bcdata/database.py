import os
from urllib.parse import urlparse
import logging

import psycopg2
from psycopg2 import sql

log = logging.getLogger(__name__)


class Database(object):
    """A simple wrapper around a psycopg connection"""

    def __init__(self, url=os.environ.get("DATABASE_URL")):
        self.url = url
        u = urlparse(url)
        db, user, password, host, port = (
            u.path[1:],
            u.username,
            u.password,
            u.hostname,
            u.port,
        )
        self.database = db
        self.user = user
        self.password = password
        self.host = host
        self.port = u.port
        self.ogr_string = f"PG:host={host} user={user} dbname={db} port={port}"
        if self.password:
            self.ogr_string = self.ogr_string + f" password={password}"
        self.conn = psycopg2.connect(url)
        # make sure postgis is available
        try:
            self.query("SELECT postgis_full_version()")
        except psycopg2.errors.UndefinedFunction:
            log.error("Cannot find PostGIS, is extension added to database %s ?", url)
            raise psycopg2.errors.UndefinedFunction

    @property
    def schemas(self):
        """List all non-system schemas in db"""
        sql = """SELECT schema_name FROM information_schema.schemata
                 ORDER BY schema_name"""
        schemas = self.query(sql)
        return [s[0] for s in schemas if s[0][:3] != "pg_"]

    @property
    def tables(self):
        """List all non-system tables in the db"""
        tables = []
        for schema in self.schemas:
            tables = tables + [schema + "." + t for t in self.tables_in_schema(schema)]
        return tables

    def tables_in_schema(self, schema):
        """Get a listing of all tables in given schema"""
        sql = """SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = %s"""
        return [t[0] for t in self.query(sql, (schema,))]

    def query(self, sql, params=None):
        """Execute sql and return all results"""
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(sql, params)
                result = curs.fetchall()
        return result

    def execute(self, sql, params=None):
        """Execute sql and return only whether the query was successful"""
        with self.conn:
            with self.conn.cursor() as curs:
                result = curs.execute(sql, params)
        return result

    def execute_many(self, sql, params):
        """Execute many sql"""
        with self.conn:
            with self.conn.cursor() as curs:
                curs.executemany(sql, params)

    def drop_pk(self, schema, table, column):
        """drop primary key constraint for given table"""
        dbq = sql.SQL(
            """
            ALTER TABLE {schema}.{table}
            ALTER COLUMN {column} DROP DEFAULT
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            column=sql.Identifier(column),
        )
        self.execute(dbq)
        dbq = sql.SQL(
            """
            ALTER TABLE {schema}.{table}
            DROP CONSTRAINT IF EXISTS {constraint}
            """
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            constraint=sql.Identifier(table + "_pkey"),
        )
        self.execute(dbq)
