from snowkill.condition.abc_condition import AbstractRunningQueryCondition
from snowkill.struct import Query, QueryPlan, CheckResultLevel


class StorageSpillingCondition(AbstractRunningQueryCondition):
    def __init__(self, *, min_local_spilling_gb: float, min_remote_spilling_gb: float, **kwargs):
        super().__init__(**kwargs)

        self.min_local_spilling_gb = min_local_spilling_gb
        self.min_remote_spilling_gb = min_remote_spilling_gb

    def check_custom_logic(self, query: Query, query_plan: QueryPlan):
        running_step = query_plan.get_running_step()

        if not running_step.statistics_spilling:
            return None

        local_spilling_gb = 0
        remote_spilling_gb = 0

        if "Bytes spilled to local storage" in running_step.statistics_spilling:
            local_spilling_gb = running_step.statistics_spilling["Bytes spilled to local storage"].value / 1024 / 1024 / 1024

        if "Bytes spilled to remote storage" in running_step.statistics_spilling:
            remote_spilling_gb = running_step.statistics_spilling["Bytes spilled to remote storage"].value / 1024 / 1024 / 1024

        if remote_spilling_gb > self.min_remote_spilling_gb:
            description = f"Query spilled at least [{remote_spilling_gb:.1f}] Gb to remote storage"
        elif local_spilling_gb > self.min_local_spilling_gb:
            description = f"Query spilled at least [{local_spilling_gb:.1f}] Gb to local storage"
        else:
            return None

        if self.kill_duration and query.execute_duration >= self.kill_duration:
            return CheckResultLevel.KILL, description

        if self.warning_duration and query.execute_duration >= self.warning_duration:
            return CheckResultLevel.WARNING, description

        if self.notice_duration and query.execute_duration >= self.notice_duration:
            return CheckResultLevel.NOTICE, description
