from snowkill.condition.abc_condition import AbstractRunningQueryCondition
from snowkill.struct import Query, QueryPlan, CheckResultLevel


class ExecuteDurationCondition(AbstractRunningQueryCondition):
    def check_custom_logic(self, query: Query, query_plan: QueryPlan):
        if self.kill_duration and query.execute_duration >= self.kill_duration:
            return CheckResultLevel.KILL, f"Query was running longer than [{self.kill_duration}] seconds"

        if self.warning_duration and query.execute_duration >= self.warning_duration:
            return CheckResultLevel.WARNING, f"Query was running longer than [{self.warning_duration}] seconds"

        if self.notice_duration and query.execute_duration >= self.notice_duration:
            return CheckResultLevel.NOTICE, f"Query was running longer than [{self.notice_duration}] seconds"
