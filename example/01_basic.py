from os import getenv
from snowflake.connector import SnowflakeConnection
from snowkill import *

from _utils import init_logger, send_slack_message

"""
Complete example featuring basic usage of SnowKill:

1) Authorise by password
2) Apply all types of checks
3) Store and deduplicate in Snowflake table
4) Format and send new check results to Slack
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
    ),
    CartesianJoinExplosionCondition(
        min_output_rows=10_000_000,  # join emits at least 10M output rows
        min_explosion_rate=10,  # ratio of output rows to input rows is at least 10x
        warning_duration=60 * 10,  # 10 minutes for warning
        kill_duration=60 * 20,  # 20 minutes for kill
    ),
    JoinExplosionCondition(
        min_output_rows=10_000_000,  # join emits at least 10M output rows
        min_explosion_rate=10,  # ratio of output rows to input rows is at least 10x
        warning_duration=60 * 10,  # 10 minutes for warning
        kill_duration=60 * 20,  # 20 minutes for kill
    ),
    UnionWithoutAllCondition(
        min_input_rows=10_000_000,  # at least 10M input rows for UNION without ALL
        notice_duration=60 * 10,  # 10 minutes for notice
    ),
    StorageSpillingCondition(
        min_local_spilling_gb=50,  # 50Gb spill to local storage
        min_remote_spilling_gb=1,  # 1Gb spill to remote storage
        warning_duration=60 * 10,  # 10 minutes for waring
        kill_duration=60 * 20,  # 20 minutes for kill
    ),
    QueuedDurationCondition(
        notice_duration=60 * 30,  # query was in queue for 30 minutes
    ),
    BlockedDurationCondition(
        notice_duration=60 * 5,  # query was locked by another transaction for 5 minutes
    ),
    EstimatedScanDurationCondition(
        min_estimated_scan_duration=60 * 60 * 2,  # query scan is estimated to take longer than 2 hours
        warning_duration=60 * 10,  # warning after 10 minutes
        kill_duration=60 * 20,  # kill after 20 minutes
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
