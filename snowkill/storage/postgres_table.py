from datetime import datetime
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from textwrap import dedent
from typing import List

from snowkill.storage.abc_storage import AbstractStorage
from snowkill.struct import CheckResult, CheckResultLevel


class PostgresTableStorage(AbstractStorage):
    """
    Postgres table structure:

    CREATE TABLE snowkill_log
    (
        query_id TEXT,
        check_result_level INTEGER,
        check_result_name TEXT,
        check_result_description TEXT,
        check_result_time TIMESTAMP WITHOUT TIME ZONE,

        PRIMARY KEY (query_id, check_result_level)
    )
    """

    def __init__(self, connection: Connection, table_name: str):
        self.connection = connection
        self.cursor = connection.cursor(row_factory=dict_row)

        self.table_name = table_name

    def store_and_remove_duplicate(self, check_results: List[CheckResult]) -> List[CheckResult]:
        if not check_results:
            return check_results

        with self.connection.transaction():
            base_query = """
                SELECT query_id, MAX(check_result_level) AS check_result_level
                FROM {table_name}
                WHERE query_id = ANY(%s)
                GROUP BY 1
            """

            formatted_query = SQL(dedent(base_query)).format(
                table_name=Identifier(self.table_name),
            )

            self.cursor.execute(formatted_query, [[r.query.query_id for r in check_results]])

            existing_result_map = {r["query_id"]: CheckResultLevel(r["check_result_level"]) for r in self.cursor}
            filtered_results = []

            for r in check_results:
                # Keep only results for new QUERY_ID's and existing QUERY_ID's with higher level
                if r.query.query_id not in existing_result_map or r.level > existing_result_map[r.query.query_id]:
                    filtered_results.append(r)

            for r in filtered_results:
                base_query = """
                    INSERT INTO {table_name}
                    (query_id, check_result_level, check_result_name, check_result_description, check_result_time)
                    VALUES (%s, %s, %s, %s, %s)
                """

                formatted_query = SQL(dedent(base_query)).format(
                    table_name=Identifier(self.table_name),
                )

                self.cursor.execute(
                    formatted_query,
                    (
                        r.query.query_id,
                        r.level.value,
                        r.name,
                        r.description,
                        datetime.utcnow(),
                    ),
                )

        return filtered_results
