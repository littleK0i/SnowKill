from snowkill import *


def test_condition_queued_duration(helper):
    query_tag = "pytest:blocked_duration"

    with helper.init_connection(query_tag) as query1_con, helper.init_connection(query_tag) as query2_con, helper.init_connection() as snowkill_con:
        query1_cur = query1_con.cursor()
        query2_cur = query2_con.cursor()

        query1_cur.execute("BEGIN")
        query2_cur.execute("BEGIN")

        query1_cur.execute_async(f"""
            UPDATE snowkill_test.public.table_1
            SET name = 'zzz'
            WHERE id = 1
        """)

        helper.sleep(1)

        query2_cur.execute_async(f"""
            UPDATE snowkill_test.public.table_1
            SET name = 'ccc'
            WHERE id = 1
        """)

        helper.sleep(30)

        try:
            engine = SnowKillEngine(snowkill_con)

            conditions = [
                BlockedDurationCondition(
                    warning_duration=10,
                    query_filter=helper.get_query_filter(query_tag)
                ),
            ]

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "BlockedDurationCondition"
            assert check_results[0].level == CheckResultLevel.WARNING
            assert check_results[0].query.query_id == query2_cur.sfqid
        finally:
            helper.kill_last_query(query1_cur)
            helper.kill_last_query(query2_cur)

            query1_cur.execute("ROLLBACK")
            query2_cur.execute("ROLLBACK")
