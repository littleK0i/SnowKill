from snowkill import *


def test_condition_join_explosion(helper):
    query_tag = "pytest:join_explosion"

    with helper.init_connection(query_tag) as query_con, helper.init_connection() as snowkill_con:
        query_cur = query_con.cursor()

        query_cur.execute_async(f"""
            SELECT *
            FROM snowflake_sample_data.tpch_sf10.orders a
                JOIN snowflake_sample_data.tpch_sf10.orders b ON (a.o_orderdate=b.o_orderdate)
        """)

        helper.sleep(30)

        try:
            engine = SnowKillEngine(snowkill_con)

            conditions = [
                JoinExplosionCondition(
                    min_output_rows=10_000,
                    min_explosion_rate=2,
                    warning_duration=10,
                    query_filter=helper.get_query_filter(query_tag)
                ),
            ]

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "JoinExplosionCondition"
            assert check_results[0].level == CheckResultLevel.WARNING
            assert check_results[0].query.query_id == query_cur.sfqid
        finally:
            query_cur.abort_query(query_cur.sfqid)
