from snowkill import *


def test_condition_queued_duration(helper):
    query_tag = "pytest:queued_duration"

    with helper.init_connection(query_tag, True) as query_con, helper.init_connection() as snowkill_con:
        query1_cur = query_con.cursor()
        query2_cur = query_con.cursor()

        query1_cur.execute_async(f"""
            SELECT *
            FROM snowflake_sample_data.tpch_sf1000.orders
            UNION
            SELECT *
            FROM snowflake_sample_data.tpch_sf100.orders
        """)

        helper.sleep(1)

        query2_cur.execute_async(f"""
            SELECT *
            FROM snowflake_sample_data.tpch_sf100.orders
            UNION
            SELECT *
            FROM snowflake_sample_data.tpch_sf1000.orders
        """)

        helper.sleep(180)

        try:
            engine = SnowKillEngine(snowkill_con)

            conditions = [
                QueuedDurationCondition(
                    warning_duration=10,
                    query_filter=helper.get_query_filter(query_tag)
                ),
            ]

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "QueuedDurationCondition"
            assert check_results[0].level == CheckResultLevel.WARNING
            assert check_results[0].query.query_id == query2_cur.sfqid
        finally:
            helper.kill_last_query(query1_cur)
            helper.kill_last_query(query2_cur)
