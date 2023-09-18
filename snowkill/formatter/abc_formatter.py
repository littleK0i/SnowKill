from abc import ABC, abstractmethod
from typing import Any

from snowkill.struct import CheckResult, Query


class AbstractFormatter(ABC):
    QUERY_TEXT_MAX_LINES = 15
    QUERY_TEXT_MAX_LINE_LENGTH = 1000

    @abstractmethod
    def format(self, result: CheckResult) -> Any:
        pass

    def _get_query_current_state_duration(self, query: Query):
        if query.status == "RUNNING":
            duration = query.execute_duration
        elif query.status == "QUEUED":
            duration = query.queued_duration
        else:
            duration = query.total_duration

        return duration

    def _get_snowsight_profile_url(self, snowsight_base_url: str, query_id: str):
        return f"{snowsight_base_url}compute/history/queries/{query_id}/profile"

    def _normalize_query_text(self, query_text: str):
        query_parts = []

        for line in query_text.splitlines():
            if len(line) > self.QUERY_TEXT_MAX_LINE_LENGTH:
                line = f"{line[:self.QUERY_TEXT_MAX_LINE_LENGTH]}..."

            query_parts.append(line)

        original_query_parts_len = len(query_parts)

        if original_query_parts_len > self.QUERY_TEXT_MAX_LINES:
            query_parts = query_parts[0 : self.QUERY_TEXT_MAX_LINES - 1]
            truncated_query_parts_len = len(query_parts)

            query_parts.append("")
            query_parts.append(f".......... [{original_query_parts_len - truncated_query_parts_len}] lines were truncated ..........")

        return "\n".join(query_parts)

    def _format_duration(self, duration: float):
        duration = int(duration)

        minutes, seconds = divmod(duration, 60)
        hours, minutes = divmod(minutes, 60)

        return f"{hours}:{minutes:02d}:{seconds:02d}"
