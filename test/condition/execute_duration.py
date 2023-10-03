from snowkill import *


def test_condition_execute_duration(helper):
    query_tag = "pytest:execute_duration"

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

            conditions = [
                ExecuteDurationCondition(
                    warning_duration=10,
                    query_filter=helper.get_query_filter(query_tag)
                ),
            ]

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "ExecuteDurationCondition"
            assert check_results[0].level == CheckResultLevel.WARNING
            assert check_results[0].query.query_id == query_cur.sfqid
        finally:
            query_cur.abort_query(query_cur.sfqid)
