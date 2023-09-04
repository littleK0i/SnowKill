from snowkill.condition.abc_condition import AbstractRunningQueryCondition
from snowkill.struct import Query, QueryPlan, CheckResultLevel


class UnionWithoutAllCondition(AbstractRunningQueryCondition):
    def __init__(self, *, min_input_rows: int, **kwargs):
        super().__init__(**kwargs)

        self.min_input_rows = min_input_rows

    def check_custom_logic(self, query: Query, query_plan: QueryPlan):
        running_step = query_plan.get_running_step()

        for node in running_step.nodes:
            # Base node is UnionAll
            if node.name != "UnionAll":
                continue

            aggregate_nodes = [dn for dn in running_step.get_downstream_nodes(node) if dn.name == "Aggregate"]

            # Exactly one downstream aggregate node
            if len(aggregate_nodes) != 1:
                continue

            # Aggregate node with Grouping Keys
            if "Grouping Keys" not in aggregate_nodes[0].labels:
                continue

            # All grouping keys starts with UNION_ALL
            if not all(str(v).startswith("UNION_ALL(") for v in aggregate_nodes[0].labels["Grouping Keys"].value):
                continue

            # Aggregate node with Aggregate Functions is not UNION, it is a normal GROUP BY
            if len(aggregate_nodes[0].labels["Aggregate Functions"].value) > 0:
                continue

            input_rows = sum(running_step.get_rows_between_nodes(un, node) for un in running_step.get_upstream_nodes(node))

            # Total number of input rows is above limit
            if input_rows < self.min_input_rows:
                continue

            description = f"UNION without ALL with at least [{input_rows}] input rows"

            if self.kill_duration and query.execute_duration >= self.kill_duration:
                return CheckResultLevel.KILL, description

            if self.warning_duration and query.execute_duration >= self.warning_duration:
                return CheckResultLevel.WARNING, description

            if self.notice_duration and query.execute_duration >= self.notice_duration:
                return CheckResultLevel.NOTICE, description
