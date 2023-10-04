from snowkill import *


def test_condition_union_without_all(helper):
    query_tag = "pytest:union_without_all"

    with helper.init_connection(query_tag) as query_con, helper.init_connection() as snowkill_con:
        query_cur = query_con.cursor()

        query_cur.execute_async(f"""
            SELECT *
            FROM snowflake_sample_data.tpch_sf100.orders
            UNION
            SELECT *
            FROM snowflake_sample_data.tpch_sf100.orders
        """)

        helper.sleep(30)

        try:
            engine = SnowKillEngine(snowkill_con)

            conditions = [
                UnionWithoutAllCondition(
                    min_input_rows=10_000,
                    warning_duration=10,
                    query_filter=helper.get_query_filter(query_tag)
                ),
            ]

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "UnionWithoutAllCondition"
            assert check_results[0].level == CheckResultLevel.WARNING
            assert check_results[0].query.query_id == query_cur.sfqid
        finally:
            helper.kill_last_query(query_cur)
