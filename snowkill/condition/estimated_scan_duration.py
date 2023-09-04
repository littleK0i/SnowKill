from snowkill.condition.abc_condition import AbstractRunningQueryCondition
from snowkill.struct import Query, QueryPlan, CheckResultLevel


class EstimatedScanDurationCondition(AbstractRunningQueryCondition):
    def __init__(self, *, min_estimated_scan_duration: int, **kwargs):
        super().__init__(**kwargs)

        self.min_estimated_scan_duration = min_estimated_scan_duration

    def check_custom_logic(self, query: Query, query_plan: QueryPlan):
        running_step = query_plan.get_running_step()

        if "Scan progress" not in running_step.statistics_io:
            return None

        scan_progress = running_step.statistics_io["Scan progress"].value

        if scan_progress == 0 or scan_progress == 1:
            return None

        estimated_scan_duration = int(running_step.duration / running_step.statistics_io["Scan progress"].value)

        if estimated_scan_duration < self.min_estimated_scan_duration:
            return None

        description = f"Estimated scan duration is at least [{int(estimated_scan_duration / 60)}] minutes"

        if self.kill_duration and query.execute_duration >= self.kill_duration:
            return CheckResultLevel.KILL, description

        if self.warning_duration and query.execute_duration >= self.warning_duration:
            return CheckResultLevel.WARNING, description

        if self.notice_duration and query.execute_duration >= self.notice_duration:
            return CheckResultLevel.NOTICE, description
