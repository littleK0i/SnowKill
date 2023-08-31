from datetime import datetime, timedelta
from snowflake.connector import DictCursor, SnowflakeConnection
from textwrap import dedent
from typing import List

from snowkill.storage.abc_storage import AbstractStorage
from snowkill.struct import CheckResult, CheckResultLevel


class SnowflakeTableStorage(AbstractStorage):
    """
    Snowflake table structure:

    CREATE TABLE xxx.xxx.snowkill_log
    (
        query_id VARCHAR(16777216),
        check_result_level NUMBER(38,0),
        check_result_name VARCHAR(16777216),
        check_result_description VARCHAR(16777216),
        check_result_time TIMESTAMP_NTZ(6)
    )
    """

    def __init__(self, connection: SnowflakeConnection, table_name: str):
        self.connection = connection
        self.cursor = connection.cursor(DictCursor)

        self.table_name = table_name

    def store_and_remove_duplicate(self, check_results: List[CheckResult]) -> List[CheckResult]:
        if not check_results:
            return check_results

        # This SELECT is intentionally simplified, so it can be consistently retrieved from QUERY_CACHE
        query = """
            SELECT query_id, MAX(check_result_level) AS check_result_level
            FROM IDENTIFIER(%(table_name)s)
            WHERE check_result_time >= %(min_check_result_time)s
            GROUP BY 1
        """

        query_params = {
            "table_name": self.table_name,
            "min_check_result_time": (datetime.utcnow() - timedelta(hours=24)).date(),
        }

        self.cursor.execute(dedent(query), query_params)

        existing_result_map = {r["QUERY_ID"]: CheckResultLevel(r["CHECK_RESULT_LEVEL"]) for r in self.cursor}
        filtered_results = []

        for r in check_results:
            # Keep only results for new QUERY_ID's and existing QUERY_ID's with higher level
            if r.query.query_id not in existing_result_map or r.level > existing_result_map[r.query.query_id]:
                filtered_results.append(r)

        if filtered_results:
            for r in filtered_results:
                query = """
                    INSERT INTO IDENTIFIER(%(table_name)s)
                    (query_id, check_result_level, check_result_name, check_result_description, check_result_time)
                    VALUES (%(query_id)s, %(check_result_level)s, %(check_result_name)s, %(check_result_description)s, %(check_result_time)s)
                """

                query_params = {
                    "table_name": self.table_name,
                    "query_id": r.query.query_id,
                    "check_result_level": r.level.value,
                    "check_result_name": r.name,
                    "check_result_description": r.description,
                    "check_result_time": datetime.utcnow(),
                }

                self.cursor.execute(dedent(query), query_params)

            self.connection.commit()

        return filtered_results
