from abc import ABC, abstractmethod
from fnmatch import fnmatchcase
from typing import List, Optional, Pattern, Tuple, Union

from snowkill.struct import Query, QueryPlan, CheckResultLevel, HoldingLock


class QueryFilter:
    def __init__(
        self,
        *,
        include_user_name: Optional[List[Union[str, Pattern]]] = None,
        exclude_user_name: Optional[List[Union[str, Pattern]]] = None,
        include_user_login_name: Optional[List[Union[str, Pattern]]] = None,
        exclude_user_login_name: Optional[List[Union[str, Pattern]]] = None,
        include_user_email: Optional[List[Union[str, Pattern]]] = None,
        exclude_user_email: Optional[List[Union[str, Pattern]]] = None,
        include_warehouse_name: Optional[List[Union[str, Pattern]]] = None,
        exclude_warehouse_name: Optional[List[Union[str, Pattern]]] = None,
        include_sql_text: Optional[List[Union[str, Pattern]]] = None,
        exclude_sql_text: Optional[List[Union[str, Pattern]]] = None,
        include_query_tag: Optional[List[Union[str, Pattern]]] = None,
        exclude_query_tag: Optional[List[Union[str, Pattern]]] = None,
    ):
        self.include_user_name = include_user_name
        self.exclude_user_name = exclude_user_name
        self.include_user_login_name = include_user_login_name
        self.exclude_user_login_name = exclude_user_login_name
        self.include_user_email = include_user_email
        self.exclude_user_email = exclude_user_email
        self.include_warehouse_name = include_warehouse_name
        self.exclude_warehouse_name = exclude_warehouse_name
        self.include_sql_text = include_sql_text
        self.exclude_sql_text = exclude_sql_text
        self.include_query_tag = include_query_tag
        self.exclude_query_tag = exclude_query_tag

    def check_query(self, query: Query):
        if self.include_user_name and not any(
            self._match_pattern(query.session.user_name, pattern) for pattern in self.include_user_name
        ):
            return False

        if self.exclude_user_name and any(
            self._match_pattern(query.session.user_name, pattern) for pattern in self.exclude_user_name
        ):
            return False

        ######

        if self.include_user_login_name and not any(
            self._match_pattern(query.user.login_name, pattern) for pattern in self.include_user_login_name
        ):
            return False

        if self.exclude_user_login_name and any(
            self._match_pattern(query.user.login_name, pattern) for pattern in self.exclude_user_login_name
        ):
            return False

        ######

        if self.include_user_email and not any(
            self._match_pattern(query.user.email, pattern) for pattern in self.include_user_email
        ):
            return False

        if self.exclude_user_email and any(self._match_pattern(query.user.email, pattern) for pattern in self.exclude_user_email):
            return False

        ######

        if self.include_warehouse_name and not any(
            self._match_pattern(query.warehouse_name, pattern) for pattern in self.include_warehouse_name
        ):
            return False

        if self.exclude_warehouse_name and any(
            self._match_pattern(query.warehouse_name, pattern) for pattern in self.exclude_warehouse_name
        ):
            return False

        ######

        if self.include_sql_text and not any(self._match_pattern(query.sql_text, pattern) for pattern in self.include_sql_text):
            return False

        if self.exclude_sql_text and any(self._match_pattern(query.sql_text, pattern) for pattern in self.exclude_sql_text):
            return False

        ######

        if self.include_query_tag and not any(
            self._match_pattern(query.query_tag, pattern) for pattern in self.include_query_tag
        ):
            return False

        if self.exclude_query_tag and any(self._match_pattern(query.query_tag, pattern) for pattern in self.exclude_query_tag):
            return False

        return True

    def _match_pattern(self, val: Optional[str], pattern: Union[str, Pattern]):
        if val is None:
            return False

        if isinstance(pattern, Pattern):
            return True if pattern.fullmatch(val) else False

        return fnmatchcase(val, pattern)


class AbstractQueryCondition(ABC):
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        notice_duration: Optional[int] = None,
        warning_duration: Optional[int] = None,
        kill_duration: Optional[int] = None,
        query_filter: Optional[QueryFilter] = None,
        enable_kill: bool = False,
        enable_kill_query_filter: Optional[QueryFilter] = None,
    ):
        self.name = name if name else self.__class__.__name__

        self.notice_duration = notice_duration
        self.warning_duration = warning_duration
        self.kill_duration = kill_duration

        self.query_filter = query_filter

        self.enable_kill = enable_kill
        self.enable_kill_query_filter = enable_kill_query_filter

    def check_query_filter(self, query: Query):
        if self.query_filter and not self.query_filter.check_query(query):
            return False

        return True

    def adjust_level(self, query: Query, level: CheckResultLevel):
        if level == CheckResultLevel.KILL:
            if not self.enable_kill:
                return CheckResultLevel.POTENTIAL_KILL

            if self.enable_kill_query_filter and not self.enable_kill_query_filter.check_query(query):
                return CheckResultLevel.POTENTIAL_KILL

        return level

    def _calculate_min_duration(self):
        all_durations = [
            self.notice_duration,
            self.warning_duration,
            self.kill_duration,
        ]

        if all(duration is None for duration in all_durations):
            raise ValueError(
                f"At least one duration should be specified for [{self.__class__.__name__}]: "
                f"notice_duration, warning_duration, kill_duration"
            )

        return min(duration for duration in all_durations if duration is not None)


class AbstractRunningQueryCondition(AbstractQueryCondition, ABC):
    @abstractmethod
    def check_custom_logic(self, query: Query, query_plan: QueryPlan) -> Optional[Tuple[CheckResultLevel, str]]:
        pass

    def check_min_duration(self, query: Query):
        return query.execute_duration >= self._calculate_min_duration()


class AbstractQueuedQueryCondition(AbstractQueryCondition, ABC):
    @abstractmethod
    def check_custom_logic(self, query: Query) -> Optional[Tuple[CheckResultLevel, str]]:
        pass

    def check_min_duration(self, query: Query):
        return query.queued_duration >= self._calculate_min_duration()


class AbstractBlockedQueryCondition(AbstractQueryCondition, ABC):
    @abstractmethod
    def check_custom_logic(
        self, query: Query, holding_lock: Optional[HoldingLock], holding_query: Optional[Query]
    ) -> Optional[Tuple[CheckResultLevel, str]]:
        pass

    def check_min_duration(self, query: Query):
        return query.total_duration >= self._calculate_min_duration()
