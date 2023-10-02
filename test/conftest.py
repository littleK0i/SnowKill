from contextlib import contextmanager
from os import environ
from pytest import fixture
from snowflake.connector import connect, SnowflakeConnection
from snowkill import QueryFilter
from time import sleep
from typing import Iterator


class Helper:
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @contextmanager
    def init_connection(self, query_tag=None) -> Iterator[SnowflakeConnection]:
        options = {
            "account": environ.get("SNOWFLAKE_ACCOUNT"),
            "user": environ.get("SNOWFLAKE_USER"),
            "password": environ.get("SNOWFLAKE_PASSWORD"),
        }

        if query_tag:
            options["session_parameters"] = {
                "QUERY_TAG": query_tag,
            }

        connection = connect(**options)

        try:
            yield connection
        finally:
            connection.close()

    def get_query_filter(self, query_tag):
        return QueryFilter(
            include_user_name=[environ.get("SNOWFLAKE_USER").upper()],
            include_query_tag=[query_tag],
        )

    def sleep(self, duration):
        sleep(duration)


@fixture(scope="session")
def helper():
    with Helper() as helper:
        yield helper
