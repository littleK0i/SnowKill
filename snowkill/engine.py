from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from ipaddress import IPv4Address
from json import loads as json_loads, JSONDecodeError
from logging import getLogger, NullHandler
from snowflake.connector import DictCursor, SnowflakeConnection, Error as SnowflakeError
from typing import Dict, List, Optional
from urllib.parse import quote, urlencode

from snowkill.condition.abc_condition import (
    AbstractQueryCondition,
    AbstractQueuedQueryCondition,
    AbstractBlockedQueryCondition,
    AbstractRunningQueryCondition,
)
from snowkill.error import SnowKillRestApiError
from snowkill.struct import (
    CheckResult,
    CheckResultLevel,
    QueryPlan,
    QueryPlanStep,
    QueryPlanEdge,
    QueryPlanWait,
    QueryPlanStatistics,
    QueryPlanLabel,
    QueryPlanNode,
    Query,
    Session,
    HoldingLock,
    User,
    dataclass_to_json_str,
)


logger = getLogger(__name__)
logger.addHandler(NullHandler())


class SnowKillEngine:
    REST_ENDPOINT_QUERY_LIST = "/monitoring/queries"
    REST_ENDPOINT_QUERY_PLAN = "/monitoring/query-plan-data"

    REST_ENDPOINT_QUERY_PLAN_TIMEOUT = 30

    STATUS_QUEUED = "QUEUED"
    STATUS_BLOCKED = "BLOCKED"
    STATUS_RUNNING = "RUNNING"

    def __init__(self, connection: SnowflakeConnection, max_workers=8):
        self.connection = connection
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=self.__class__.__name__)
        self.logger = logger

        self._user_cache: Dict[str, User] = {}
        self._query_plan_cache: Dict[str, QueryPlan] = {}

        self._reload_user_cache()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown()

    def check_and_kill_pending_queries(self, conditions: List[AbstractQueryCondition]) -> List[CheckResult]:
        self._reset_query_plan_cache()

        check_results = []

        blocked_conditions = [c for c in conditions if isinstance(c, AbstractBlockedQueryCondition)]
        queued_conditions = [c for c in conditions if isinstance(c, AbstractQueuedQueryCondition)]
        running_conditions = [c for c in conditions if isinstance(c, AbstractRunningQueryCondition)]

        pending_queries = self.get_pending_queries(
            blocked=len(blocked_conditions) > 0,
            queued=len(queued_conditions) > 0,
            running=True,
        )

        if any(query.status == self.STATUS_BLOCKED for query in pending_queries.values()):
            holding_locks = self.get_holding_locks()

        # This sub-function runs in parallel by ThreadPoolExecutor below
        # It helps to mitigate query_plan performance issues
        def _thread_inner_fn(query: Query):
            results = []

            if query.status == self.STATUS_BLOCKED:
                results = [self._check_blocked_query(c, query, holding_locks.get(query.query_id)) for c in blocked_conditions]

            if query.status == self.STATUS_QUEUED:
                results = [self._check_queued_query(c, query) for c in queued_conditions]

            if query.status == self.STATUS_RUNNING:
                results = [self._check_running_query(c, query) for c in running_conditions]

            # Remove empty results
            results = [r for r in results if r is not None]

            if results:
                result_with_highest_level = max(results, key=lambda r: r.level)

                if result_with_highest_level.level == CheckResultLevel.KILL:
                    self.connection.cursor().abort_query(query.query_id)

                yield result_with_highest_level

        for gen in self.executor.map(_thread_inner_fn, pending_queries.values()):
            check_results.extend(gen)

        return check_results

    def _check_blocked_query(self, condition: AbstractBlockedQueryCondition, query: Query, holding_lock: Optional[HoldingLock]):
        if not condition.check_min_duration(query):
            return None

        if not condition.check_query_filter(query):
            return None

        holding_query = self.get_query_by_id(holding_lock.holding_query_id) if holding_lock else None
        result = condition.check_custom_logic(query, holding_lock, holding_query)

        if not result:
            return None

        level, description = result
        level = condition.adjust_level(query, level)

        return CheckResult(
            level=level,
            name=condition.name,
            description=description,
            query=query,
            holding_lock=holding_lock,
            holding_query=holding_query,
        )

    def _check_queued_query(self, condition: AbstractQueuedQueryCondition, query: Query):
        if not condition.check_min_duration(query):
            return None

        if not condition.check_query_filter(query):
            return None

        result = condition.check_custom_logic(query)

        if not result:
            return None

        level, description = result
        level = condition.adjust_level(query, level)

        return CheckResult(
            level=level,
            name=condition.name,
            description=description,
            query=query,
        )

    def _check_running_query(self, condition: AbstractRunningQueryCondition, query: Query):
        if not condition.check_min_duration(query):
            return None

        if not condition.check_query_filter(query):
            return None

        query_plan = self._get_query_plan_from_cache(query.query_id)

        if not query_plan or not query_plan.get_running_step():
            return None

        result = condition.check_custom_logic(query, self._query_plan_cache[query.query_id])

        if not result:
            return None

        level, description = result
        level = condition.adjust_level(query, level)

        return CheckResult(
            level=level,
            name=condition.name,
            description=description,
            query=query,
            query_plan=query_plan,
        )

    def get_pending_queries(self, *, blocked=True, queued=True, running=True) -> Dict[str, Query]:
        pending_queries = {}

        if blocked:
            pending_queries.update(self._list_queries(subset=self.STATUS_BLOCKED))

        if queued:
            pending_queries.update(self._list_queries(subset=self.STATUS_QUEUED))

        if running:
            pending_queries.update(self._list_queries(subset=self.STATUS_RUNNING))

        return pending_queries

    def get_query_by_id(self, query_id: str) -> Optional[Query]:
        queries = self._list_queries(query_id=query_id)

        if not queries:
            return None

        return queries[query_id]

    def _reload_user_cache(self):
        self._user_cache = {}

        cursor = self.connection.cursor(DictCursor)
        cursor.execute("SHOW USERS")

        for r in cursor:
            self._user_cache[r["name"]] = User(
                name=r["name"],
                login_name=r["login_name"],
                display_name=r["display_name"],
                first_name=r["first_name"] if r["first_name"] else None,
                last_name=r["last_name"] if r["last_name"] else None,
                email=r["email"] if r["email"] else None,
                comment=r["comment"] if r["comment"] else None,
                default_warehouse=r["default_warehouse"] if r["default_warehouse"] else None,
                default_role=r["default_role"] if r["default_role"] else None,
                owner=r["owner"] if r["owner"] else None,
            )

    def _reset_query_plan_cache(self):
        self._query_plan_cache = {}

    def _get_user_from_cache(self, user_name: str):
        # User with this name was not returned from SHOW USERS
        # It may happen due to race condition
        if user_name not in self._user_cache:
            self._user_cache[user_name] = User(
                name=user_name,
                login_name=None,
                display_name=None,
                first_name=None,
                last_name=None,
                email=None,
                comment=None,
                default_warehouse=None,
                default_role=None,
                owner=None,
            )

        return self._user_cache[user_name]

    def _get_query_plan_from_cache(self, query_id: str):
        if query_id not in self._query_plan_cache:
            self._query_plan_cache[query_id] = self.get_query_plan(query_id)

        return self._query_plan_cache[query_id]

    def _list_queries(self, subset: Optional[str] = None, query_id: Optional[str] = None):
        url_params = {
            "max": 1000,
            "internal": "false",
            "scheduled_replication_task_jobs": "false",
            "start": self._datetime_to_int(datetime.utcnow() - timedelta(hours=24)),
        }

        if subset:
            url_params["subset"] = subset

        if query_id:
            url_params["uuid"] = query_id

        response = self.connection.rest.request(
            url=f"{self.REST_ENDPOINT_QUERY_LIST}?{urlencode(url_params)}",
            method="get",
            client="rest",
        )

        if not response.get("success"):
            raise SnowKillRestApiError(response.get("code"), response.get("message"))

        sessions = {}
        queries = {}

        for s in response["data"]["sessionsShort"]:
            sessions[s["idAsString"]] = Session(
                session_id=s["idAsString"],
                client_application=s["clientApplication"],
                client_environment=self._try_parse_json(s["clientEnvironment"]),
                client_net_address=IPv4Address(s["clientNetAddress"]),
                client_support_info=s["clientSupportInfo"],
                user_name=s["userName"],
            )

        for q in response["data"]["queries"]:
            # Skip queries in compiling state
            if q["state"] == "GS_COMPILING":
                continue

            queries[q["id"]] = Query(
                query_id=q["id"],
                query_tag=q["queryTag"],
                sql_text=q["sqlText"],
                status=q["status"],
                state=q["state"],
                session=sessions[q["sessionIdAsString"]],
                user=self._get_user_from_cache(sessions[q["sessionIdAsString"]].user_name),
                client_send_time=self._int_to_datetime(q["clientSendTime"]),
                start_time=self._int_to_datetime(q["startTime"]),
                end_time=self._int_to_datetime(q["endTime"]),
                compile_duration=q["gsCompileDuration"] / 1000,
                execute_duration=(q["gsExecDuration"] + q["xpExecDuration"]) / 1000,
                queued_duration=q.get("stats", {}).get("queuedLoadTime", 0) / 1000,
                listing_external_file_duration=q["listingExternalFiles"] / 1000,
                total_duration=q["totalDuration"] / 1000,
                warehouse_id=q["warehouseId"],
                warehouse_name=q["warehouseName"],
                warehouse_external_size=q["warehouseExternalSize"],
                warehouse_server_type=q["warehouseServerType"],
                stats=q.get("stats", {}),
                meta_version=q["metaVersion"],
                snowflake_version=(
                    q["majorVersionNumber"],
                    q["minorVersionNumber"],
                    q["patchVersionNumber"],
                ),
            )

        return queries

    def get_query_plan(self, query_id: str):
        try:
            response = self.connection.rest.request(
                url=f"{self.REST_ENDPOINT_QUERY_PLAN}/{quote(query_id)}",
                method="get",
                client="rest",
                timeout=self.REST_ENDPOINT_QUERY_PLAN_TIMEOUT,
                _no_retry=True,
            )
        except SnowflakeError as e:
            logger.warning(f"Could not load query plan for query_id [{query_id}] due to [{e.__class__.__name__}]")
            response = None

        # Request was terminated due to error or timeout
        # Query plan is not available
        if not response:
            return None

        # Something is wrong with response, attention is required
        if not response.get("success"):
            raise SnowKillRestApiError(response.get("code"), response.get("message"))

        steps = []

        for s in response["data"]["steps"]:
            steps.append(self._build_query_plan_step(s))

        return QueryPlan(steps=steps)

    def get_holding_locks(self):
        cursor = self.connection.cursor(DictCursor)
        cursor.execute("SHOW LOCKS IN ACCOUNT")

        holding_row_cache = {}
        holding_locks = {}

        for r in cursor:
            key = (r["resource"], r["type"])

            if r["status"] == "HOLDING":
                holding_row_cache[key] = r
            elif r["status"] == "WAITING" and key in holding_row_cache:
                holding_locks[r["query_id"]] = HoldingLock(
                    waiting_query_id=r["query_id"],
                    waiting_session_id=r["session"],
                    waiting_transaction_id=r["transaction"],
                    holding_query_id=holding_row_cache[key]["query_id"],
                    holding_session_id=holding_row_cache[key]["session"],
                    holding_transaction_id=holding_row_cache[key]["transaction"],
                    resource=r["resource"],
                    type=r["type"],
                )

        return holding_locks

    def _build_query_plan_step(self, step_def: dict):
        nodes = []
        edges = []
        labels = {}
        waits = {}
        statistics_io = {}
        statistics_pruning = {}
        statistics_spilling = {}

        for item in step_def["graphData"]["nodes"]:
            nodes.append(self._build_query_plan_node(item))

        for item in step_def["graphData"]["edges"]:
            edges.append(
                QueryPlanEdge(
                    id=item["id"],
                    src=item["src"],
                    dst=item["dst"],
                    rows=item["rows"],
                    expressions=item["expressions"],
                )
            )

        for item in step_def["graphData"].get("labels", []):
            labels[item["name"]] = QueryPlanLabel(
                name=item["name"],
                value=item["value"],
            )

        for item in step_def["graphData"]["global"].get("waits", []):
            waits[item["name"]] = QueryPlanWait(
                name=item["name"],
                value=item["value"],
                percentage=item["percentage"],
            )

        for item in step_def["graphData"]["global"].get("statistics", {}).get("IO", []):
            statistics_io[item["name"]] = QueryPlanStatistics(
                name=item["name"],
                value=item["value"],
                unit=item["unit"],
            )

        for item in step_def["graphData"]["global"].get("statistics", {}).get("Pruning", []):
            statistics_pruning[item["name"]] = QueryPlanStatistics(
                name=item["name"],
                value=item["value"],
                unit=item["unit"],
            )

        for item in step_def["graphData"]["global"].get("statistics", {}).get("Spilling", []):
            statistics_spilling[item["name"]] = QueryPlanStatistics(
                name=item["name"],
                value=item["value"],
                unit=item["unit"],
            )

        return QueryPlanStep(
            step=step_def["step"],
            description=step_def["description"],
            duration=step_def["timeInMs"] / 1000,
            state=step_def["state"],
            nodes=nodes,
            edges=edges,
            labels=labels,
            waits=waits,
            statistics_io=statistics_io,
            statistics_pruning=statistics_pruning,
            statistics_spilling=statistics_spilling,
        )

    def _build_query_plan_node(self, node_def: dict):
        labels = {}
        waits = {}
        statistics_io = {}
        statistics_pruning = {}

        for item in node_def.get("labels", []):
            labels[item["name"]] = QueryPlanLabel(
                name=item["name"],
                value=item["value"],
            )

        for item in node_def.get("waits", []):
            waits[item["name"]] = QueryPlanWait(
                name=item["name"],
                value=item["value"],
                percentage=item["percentage"],
            )

        for item in node_def.get("statistics", {}).get("IO", []):
            statistics_io[item["name"]] = QueryPlanStatistics(
                name=item["name"],
                value=item["value"],
                unit=item["unit"],
            )

        for item in node_def.get("statistics", {}).get("Pruning", []):
            statistics_pruning[item["name"]] = QueryPlanStatistics(
                name=item["name"],
                value=item["value"],
                unit=item["unit"],
            )

        return QueryPlanNode(
            id=node_def["id"],
            logical_id=node_def["logicalId"],
            name=node_def["name"],
            title=node_def.get("title"),
            labels=labels,
            waits=waits,
            statistics_io=statistics_io,
            statistics_pruning=statistics_pruning,
        )

    def _try_parse_json(self, val: str):
        try:
            return json_loads(val)
        except JSONDecodeError:
            return {}

    def _datetime_to_int(self, val: datetime):
        return int(val.timestamp() * 1000)

    def _int_to_datetime(self, val: int):
        if val == 0:
            return None

        return datetime.utcfromtimestamp(val / 1000)
