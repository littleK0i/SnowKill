from snowkill.formatter.abc_formatter import AbstractFormatter
from snowkill.struct import CheckResult, CheckResultLevel


class SlackFormatter(AbstractFormatter):
    HEADER_EMOJI_MAPPING = {
        CheckResultLevel.NOTICE: ":eyes:",
        CheckResultLevel.WARNING: ":warning:",
        CheckResultLevel.POTENTIAL_KILL: ":ghost:",
        CheckResultLevel.KILL: ":skull_and_crossbones:",
    }

    HEADER_TEXT_MAPPING = {
        CheckResultLevel.NOTICE: "query raised a notice",
        CheckResultLevel.WARNING: "query raised a warning",
        CheckResultLevel.POTENTIAL_KILL: "query would be terminated",
        CheckResultLevel.KILL: "query was terminated",
    }

    def __init__(self, snowsight_base_url):
        self.snowsight_base_url = snowsight_base_url

    def format(self, result: CheckResult):
        blocks = []

        blocks.extend(self._get_header_blocks(result))
        blocks.extend(self._get_description_blocks(result))
        blocks.extend(self._get_query_profile_link_blocks(result))
        blocks.extend(self._get_query_text_blocks(result))
        blocks.extend(self._get_query_stat_blocks(result))

        if result.holding_lock:
            blocks.extend(self._get_divider_blocks())
            blocks.extend(self._get_holding_lock_blocks(result))

            if result.holding_query:
                blocks.extend(self._get_holding_query_text_blocks(result))
                blocks.extend(self._get_holding_query_stat_blocks(result))

        return blocks

    def _get_divider_blocks(self):
        return [
            {
                "type": "divider",
            }
        ]

    def _get_header_blocks(self, result: CheckResult):
        return [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{self.HEADER_EMOJI_MAPPING[result.level]} {result.query.status} {self.HEADER_TEXT_MAPPING[result.level]}",
                    "emoji": True,
                },
            }
        ]

    def _get_description_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"```{self._replace_triple_backticks(result.description)}```"},
            }
        ]

    def _get_query_profile_link_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Profile:* <{self._get_snowsight_profile_url(self.snowsight_base_url, result.query.query_id)}|{result.query.query_id}>",
                },
            }
        ]

    def _get_query_text_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{self._replace_triple_backticks(self._normalize_query_text(result.query.sql_text))}```",
                },
            },
        ]

    def _get_query_stat_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*User*: `{result.query.session.user_name}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Duration*: `{self._format_duration(self._get_query_current_state_duration(result.query))}`",
                    },
                ],
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Warehouse*: `{result.query.warehouse_name if self._replace_backticks(result.query.warehouse_name) else '-'}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Application*: `{result.query.session.client_application if self._replace_backticks(result.query.session.client_application) else '-'}`",
                    },
                ],
            },
        ]

    def _get_holding_lock_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Blocked by:* <{self._get_snowsight_profile_url(self.snowsight_base_url, result.holding_lock.holding_query_id)}|{result.holding_lock.holding_query_id}>",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Blocked on:* `{self._replace_backticks(result.holding_lock.resource)} ({result.holding_lock.type})`",
                },
            },
        ]

    def _get_holding_query_text_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{self._replace_triple_backticks(self._normalize_query_text(result.holding_query.sql_text))}```",
                },
            }
        ]

    def _get_holding_query_stat_blocks(self, result: CheckResult):
        return [
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*User*: `{self._replace_backticks(result.holding_query.session.user_name)}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Status*: `{self._replace_backticks(result.holding_query.status)}`",
                    },
                ],
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Warehouse*: `{result.holding_query.warehouse_name if self._replace_backticks(result.holding_query.warehouse_name) else '-'}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Application*: `{result.holding_query.session.client_application if self._replace_backticks(result.holding_query.session.client_application) else '-'}`",
                    },
                ],
            },
        ]

    def _replace_backticks(self, val):
        # TODO: consider alternative approach with multiple backticks
        return str(val).replace("`", "'")

    def _replace_triple_backticks(self, val):
        # TODO: consider alternative approach with multiple backticks
        return str(val).replace("```", "'''")
