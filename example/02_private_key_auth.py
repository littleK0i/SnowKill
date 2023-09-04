from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from os import getenv
from snowflake.connector import SnowflakeConnection
from snowkill import *

from _utils import init_logger, send_slack_message

"""
Complete example featuring authorization by private key
https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#label-python-key-pair-authn-rotation
"""
logger = init_logger()

private_key = serialization.load_pem_private_key(
    data=getenv("SNOWFLAKE_PRIVATE_KEY").encode(),
    password=None,
    backend=default_backend(),
)

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

connection = SnowflakeConnection(
    account=getenv("SNOWFLAKE_ACCOUNT"),
    user=getenv("SNOWFLAKE_USER"),
    private_key=private_key_bytes,
)

snowkill_engine = SnowKillEngine(connection)
snowkill_storage = SnowflakeTableStorage(connection, getenv("SNOWFLAKE_TARGET_TABLE"))
snowkill_formatter = SlackFormatter(getenv("SNOWSIGHT_BASE_URL"))

checks = [
    ExecuteDurationCondition(
        warning_duration=60 * 30,  # 30 minutes for warning
        kill_duration=60 * 60,  # 60 minutes for kill
    ),
]

# Apply checks to running, queued, blocked queries
check_results = snowkill_engine.check_and_kill_pending_queries(checks)
logger.info(f"[{len(check_results)}] queries matched check conditions")

# Save successful checks in storage and remove duplicates
# check_results = snowkill_storage.store_and_remove_duplicate(check_results)
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
