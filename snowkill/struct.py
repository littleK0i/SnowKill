from dataclasses import dataclass, is_dataclass, fields
from datetime import datetime
from enum import Enum, IntEnum
from json import dumps
from ipaddress import IPv4Address
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class HoldingLock:
    waiting_query_id: str
    waiting_session_id: str
    waiting_transaction_id: str

    holding_query_id: str
    holding_session_id: str
    holding_transaction_id: str

    resource: str
    type: str


@dataclass
class Session:
    session_id: str

    client_application: str
    client_environment: Dict[str, Any]
    client_net_address: IPv4Address
    client_support_info: str

    user_name: str


@dataclass
class User:
    name: str
    login_name: Optional[str]
    display_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    comment: Optional[str]

    default_warehouse: Optional[str]
    default_role: Optional[str]
    owner: Optional[str]


@dataclass
class Query:
    query_id: str
    query_tag: str

    sql_text: str

    status: str
    state: str

    session: Session
    user: User

    client_send_time: datetime
    start_time: Optional[datetime]
    end_time: Optional[datetime]

    compile_duration: float
    execute_duration: float
    queued_duration: float
    listing_external_file_duration: float
    total_duration: float

    warehouse_id: Optional[int]
    warehouse_name: Optional[str]
    warehouse_external_size: Optional[str]
    warehouse_server_type: Optional[str]

    stats: Dict[str, Any]

    meta_version: int
    snowflake_version: Tuple[int, int, int]


@dataclass
class QueryPlanLabel:
    name: str
    value: Any


@dataclass
class QueryPlanStatistics:
    name: str
    value: float
    unit: str


@dataclass
class QueryPlanWait:
    name: str
    value: float
    percentage: float


@dataclass
class QueryPlanNode:
    id: int
    logical_id: int
    name: str
    title: Optional[str]
    labels: Dict[str, QueryPlanLabel]
    waits: Dict[str, QueryPlanWait]
    statistics_io: Dict[str, QueryPlanStatistics]
    statistics_pruning: Dict[str, QueryPlanStatistics]


@dataclass
class QueryPlanEdge:
    id: str
    src: int
    dst: int
    rows: int
    expressions: Any


@dataclass
class QueryPlanStep:
    step: int
    description: str
    duration: float
    state: str
    nodes: List[QueryPlanNode]
    edges: List[QueryPlanEdge]
    labels: Dict[str, QueryPlanLabel]
    waits: Dict[str, QueryPlanWait]
    statistics_io: Dict[str, QueryPlanStatistics]
    statistics_pruning: Dict[str, QueryPlanStatistics]
    statistics_spilling: Dict[str, QueryPlanStatistics]

    def get_upstream_nodes(self, node: QueryPlanNode) -> List[QueryPlanNode]:
        upstream_node_ids = [e.src for e in self.edges if node.id == e.dst]
        return [n for n in self.nodes if n.id in upstream_node_ids]

    def get_downstream_nodes(self, node: QueryPlanNode) -> List[QueryPlanNode]:
        downstream_node_ids = [e.dst for e in self.edges if node.id == e.src]
        return [n for n in self.nodes if n.id in downstream_node_ids]

    def get_rows_between_nodes(self, upstream_node: QueryPlanNode, downstream_node: QueryPlanNode) -> Optional[int]:
        for e in self.edges:
            if upstream_node.id == e.src and downstream_node.id == e.dst:
                return e.rows

        return None


@dataclass
class QueryPlan:
    steps: List[QueryPlanStep]

    def get_running_step(self):
        for s in self.steps:
            if s.state == "running":
                return s

        return None


class CheckResultLevel(IntEnum):
    NOTICE = 1
    WARNING = 2
    POTENTIAL_KILL = 3
    KILL = 4


@dataclass
class CheckResult:
    level: CheckResultLevel
    name: str
    description: str
    query: Query
    query_plan: Optional[QueryPlan] = None
    holding_lock: Optional[HoldingLock] = None
    holding_query: Optional[Query] = None


def dataclass_to_json_str(val):
    return dumps(dataclass_to_dict_recursive(val), indent=2, default=str)


def dataclass_to_dict_recursive(val):
    if isinstance(val, list):
        return [dataclass_to_dict_recursive(v) for v in val]

    if isinstance(val, dict):
        return {k: dataclass_to_dict_recursive(v) for k, v in val.items()}

    if isinstance(val, Enum):
        return val.name

    if is_dataclass(val):
        return {f.name: dataclass_to_dict_recursive(getattr(val, f.name)) for f in fields(val)}

    return val
