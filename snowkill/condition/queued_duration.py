from snowkill.condition.abc_condition import AbstractQueuedQueryCondition
from snowkill.struct import Query, CheckResultLevel


class QueuedDurationCondition(AbstractQueuedQueryCondition):
    def check_custom_logic(self, query: Query):
        if self.kill_duration and query.queued_duration >= self.kill_duration:
            return CheckResultLevel.KILL, f"Query was queued longer than [{self.kill_duration}] seconds"

        if self.warning_duration and query.queued_duration >= self.warning_duration:
            return CheckResultLevel.WARNING, f"Query was queued longer than [{self.warning_duration}] seconds"

        if self.notice_duration and query.queued_duration >= self.notice_duration:
            return CheckResultLevel.NOTICE, f"Query was queued longer than [{self.notice_duration}] seconds"
