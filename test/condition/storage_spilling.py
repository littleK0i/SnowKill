from snowkill import *


def test_condition_storage_spilling(helper):
    query_tag = "pytest:storage_spilling"

    with helper.init_connection(query_tag) as query_con, helper.init_connection() as snowkill_con:
        query_cur = query_con.cursor()

        query_cur.execute_async(f"""
            SELECT *
            FROM snowflake_sample_data.tpch_sf10.orders a
                JOIN snowflake_sample_data.tpch_sf10.orders b ON (a.o_custkey > b.o_custkey)
        """)

        helper.sleep(120)

        try:
            engine = SnowKillEngine(snowkill_con)

            conditions = [
                StorageSpillingCondition(
                    min_local_spilling_gb=0.1,
                    min_remote_spilling_gb=0,
                    warning_duration=10,
                    query_filter=helper.get_query_filter(query_tag)
                ),
            ]

            print(dataclass_to_json_str(engine.get_query_plan(query_cur.sfqid)))

            check_results = engine.check_and_kill_pending_queries(conditions)

            assert len(check_results) == 1

            assert check_results[0].name == "StorageSpillingCondition"
            assert check_results[0].level == CheckResultLevel.WARNING
            assert check_results[0].query.query_id == query_cur.sfqid
        finally:
            query_cur.abort_query(query_cur.sfqid)
