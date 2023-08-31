from typing import Optional

from snowkill.checker.abc_checker import AbstractBlockedQueryChecker
from snowkill.struct import Query, CheckResultLevel, HoldingLock


class BlockedDurationChecker(AbstractBlockedQueryChecker):
    def check_custom_logic(
        self,
        waiting_query: Query,
        holding_lock: Optional[HoldingLock],
        holding_query: Optional[Query],
    ):
        if self.kill_duration and waiting_query.total_duration >= self.kill_duration:
            return (
                CheckResultLevel.KILL,
                f"Query was blocked longer than [{self.kill_duration}] seconds",
            )

        if self.warning_duration and waiting_query.total_duration >= self.warning_duration:
            return (
                CheckResultLevel.WARNING,
                f"Query was blocked longer than [{self.warning_duration}] seconds",
            )

        if self.notice_duration and waiting_query.total_duration >= self.notice_duration:
            return (
                CheckResultLevel.NOTICE,
                f"Query was blocked longer than [{self.notice_duration}] seconds",
            )
