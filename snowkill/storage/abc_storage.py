from abc import ABC, abstractmethod
from typing import List

from snowkill.struct import CheckResult


class AbstractStorage(ABC):
    @abstractmethod
    def store_and_remove_duplicate(self, check_results: List[CheckResult]) -> List[CheckResult]:
        pass
