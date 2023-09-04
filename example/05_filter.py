from os import getenv
from snowflake.connector import SnowflakeConnection
from snowkill import *

from _utils import init_logger, send_slack_message

"""
Complete example featuring query filters
"""
logger = init_logger()

connection = SnowflakeConnection(
    account=getenv("SNOWFLAKE_ACCOUNT"),
    user=getenv("SNOWFLAKE_USER"),
    password=getenv("SNOWFLAKE_PASSWORD"),
)

snowkill_engine = SnowKillEngine(connection)
snowkill_storage = SnowflakeTableStorage(connection, getenv("SNOWFLAKE_TARGET_TABLE"))
snowkill_formatter = SlackFormatter(getenv("SNOWSIGHT_BASE_URL"))

checks = [
    ExecuteDurationCondition(
        warning_duration=60 * 30,  # 30 minutes for warning
        kill_duration=60 * 60,  # 60 minutes for kill
        query_filter=QueryFilter(
            include_user_name=["STARTUP_*"],  # Apply to users with names starting with prefix `STARTUP_`
            exclude_user_name=["STARTUP_ADMIN", "STARTUP_SCRIPT"],  # Exclude some specific users
        ),
    ),
    QueuedDurationCondition(
        notice_duration=60 * 30,  # query was in queue for 30 minutes
        kill_duration=60 * 60 * 3,  # query was in queue for 3 hours
        query_filter=QueryFilter(
            include_warehouse_name=["ANALYST_WH", "LOOKER_WH"],  # Apply to specific warehouses
            include_query_tag=["*some_fancy_tag*"],  # Apply to specific query tag
        ),
        enable_kill=True,  # queries can be actually killed
        enable_kill_query_filter=QueryFilter(
            exclude_sql_text=["*--no-kill*"],  # Do not kill queries with SQL comment `--no-kill` (stop word)
        ),
    ),
]

# Apply checks to running, queued, blocked queries
check_results = snowkill_engine.check_and_kill_pending_queries(checks)
logger.info(f"[{len(check_results)}] queries matched check conditions")

# Save successful checks in storage and remove duplicates
check_results = snowkill_storage.store_and_remove_duplicate(check_results)
logger.info(f"[{len(check_results)}] queries remained after store deduplication")

# Send notification for each new check result
for r in check_results:
    response = send_slack_message(
        slack_token=getenv("SLACK_TOKEN"),
        slack_channel=getenv("SLACK_CHANNEL"),
        message_blocks=snowkill_formatter.format(r),
    )

    if response["ok"]:
        logger.info(f"Sent Slack notification for query [{r.query.query_id}]")
    else:
        logger.warning(f"Failed to send Slack notification for query [{r.query.query_id}], error: [{response['error']}]")
