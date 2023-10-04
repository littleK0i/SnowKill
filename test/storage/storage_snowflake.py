from snowkill import *


def test_storage_snowflake(helper):
    query_tag = "pytest:storage_snowflake"

    with helper.init_connection(query_tag) as query_con, helper.init_connection() as snowkill_con:
        query_cur = query_con.cursor()

        query_cur.execute_async(f"""
                SELECT *
                FROM snowflake_sample_data.tpch_sf100.customer a
                    JOIN snowflake_sample_data.tpch_sf100.customer b ON (a.c_custkey > b.c_custkey)
            """)

        helper.sleep(30)

        try:
            engine = SnowKillEngine(snowkill_con)
            storage = SnowflakeTableStorage(snowkill_con, "snowkill_test.public.snowkill_log")

            conditions = [
                ExecuteDurationCondition(
                    notice_duration=10,
                    query_filter=helper.get_query_filter(query_tag),
                ),
            ]

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "ExecuteDurationCondition"
            assert check_results[0].level == CheckResultLevel.NOTICE
            assert check_results[0].query.query_id == query_cur.sfqid

            # Store NOTICE
            processed_check_results = storage.store_and_remove_duplicate(check_results)
            assert len(processed_check_results) == 1

            # Deduplicate second attempt to store NOTICE
            processed_check_results = storage.store_and_remove_duplicate(check_results)
            assert len(processed_check_results) == 0

            check_results[0].level = CheckResultLevel.WARNING

            # Store WARNING
            processed_check_results = storage.store_and_remove_duplicate(check_results)
            assert len(processed_check_results) == 1

        finally:
            query_cur.abort_query(query_cur.sfqid)
