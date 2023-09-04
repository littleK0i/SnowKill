from snowkill.condition.abc_condition import AbstractRunningQueryCondition
from snowkill.struct import Query, QueryPlan, CheckResultLevel


class JoinExplosionCondition(AbstractRunningQueryCondition):
    def __init__(self, *, min_output_rows: int, min_explosion_rate: float, **kwargs):
        super().__init__(**kwargs)

        self.min_output_rows = min_output_rows
        self.min_explosion_rate = min_explosion_rate

    def check_custom_logic(self, query: Query, query_plan: QueryPlan):
        running_step = query_plan.get_running_step()

        for node in running_step.nodes:
            # Base node is CartesianJoin
            if node.name != "Join":
                continue

            input_rows = sum(running_step.get_rows_between_nodes(un, node) for un in running_step.get_upstream_nodes(node))
            output_rows = sum(running_step.get_rows_between_nodes(node, dn) for dn in running_step.get_downstream_nodes(node))

            explosion_rate = output_rows / input_rows if input_rows > 0 else 0

            if output_rows > self.min_output_rows and explosion_rate > self.min_explosion_rate:
                description = f"Join with explosion rate [{explosion_rate:.3f}]"

                if "Equality Join Condition" in node.labels:
                    description += f" on [{node.labels['Equality Join Condition'].value}]"

                if self.kill_duration and query.execute_duration >= self.kill_duration:
                    return CheckResultLevel.KILL, description

                if self.warning_duration and query.execute_duration >= self.warning_duration:
                    return CheckResultLevel.WARNING, description

                if self.notice_duration and query.execute_duration >= self.notice_duration:
                    return CheckResultLevel.NOTICE, description
