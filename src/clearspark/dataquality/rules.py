
from clearspark.dataquality.result import (
    RuleResult, 
    RuleValidationError
)

from clearspark.validation.annotations import (
    DuckSparkColumn, 
    DuckSparkDataFrame
)

from abc import (
    ABC, 
    abstractmethod
)

from typing import (
    Any, 
    Union
)

import math
import pyspark.sql.functions as F

__all__ = [
    'Rule',
    'Null',
    'NotNull',
    'LessThan',
    'MoreThan',
    'Equal',
    'NotEqual',
    'Unique',
    'Match',
    'NotMatch',
    'Expr'
]


class Rule(ABC):
    """Abstract base class for all data quality rules.

    Subclasses implement `_count_failures`, which returns only the
    count of rows that fail the rule (i.e. rows that do NOT satisfy
    the expectation the rule describes). The base `verify` method
    handles tolerance comparison, exception raising, and result
    packaging, so subclasses never need to repeat that logic.

    Attributes:
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. If an int, it represents an absolute row count.
            If a float (e.g., 0.05), it represents a percentage of total rows.
        raise_exception: Whether to raise `RuleValidationError`
            immediately when the rule fails (beyond tolerance).
    """

    def __init__(self, tolerance: Union[int, float] = 0, raise_exception: bool = False):
        self.tolerance = tolerance
        self.raise_exception = raise_exception

    @property
    def name(self) -> str:
        """Name of the rule, used in reports (e.g. 'LessThan')."""
        return self.__class__.__name__

    @property
    def details(self) -> dict[str, Any]:
        """Extra parameters of this rule, included in `RuleResult`.

        Subclasses with parameters (e.g. `value` in `LessThan`)
        override this to surface them in reports. Defaults to empty.
        """
        return {}

    @abstractmethod
    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        """Counts rows that fail this rule."""
        raise NotImplementedError

    def verify(
        self,
        column: DuckSparkColumn,
        column_name: str,
        total_count: int,
        df: DuckSparkDataFrame,
    ) -> RuleResult:
        """Runs the rule and packages the outcome into a RuleResult."""
        failed_count = self._count_failures(column=column, df=df)

        if isinstance(self.tolerance, float):
            allowed_failures = math.floor(self.tolerance * total_count)
        else:
            allowed_failures = self.tolerance

        result = RuleResult(
            rule_name=self.name,
            column_name=column_name,
            total_count=total_count,
            failed_count=failed_count,
            tolerance=allowed_failures,
            details=self.details,
        )

        if not result.passed and self.raise_exception:
            raise RuleValidationError(result)

        return result

    def __repr__(self) -> str:
        suffix = f"{self.tolerance * 100}%" if isinstance(self.tolerance, float) else self.tolerance
        return (
            f"{self.name}(tolerance={suffix}, "
            f"raise_exception={self.raise_exception})"
        )


class Null(Rule):
    """Expects column values to be null."""
    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column.isNotNull()).count()


class NotNull(Rule):
    """Expects column values to be non-null."""
    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column.isNull()).count()


class LessThan(Rule):
    """Expects column values to be strictly less than a given threshold."""
    def __init__(self, value: Union[int, float], tolerance: Union[int, float] = 0, raise_exception: bool = False):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.value = value

    @property
    def details(self) -> dict[str, Any]:
        return {"value": self.value}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column >= self.value).count()


class MoreThan(Rule):
    """Expects column values to be strictly greater than a given threshold."""
    def __init__(self, value: Union[int, float], tolerance: Union[int, float] = 0, raise_exception: bool = False):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.value = value

    @property
    def details(self) -> dict[str, Any]:
        return {"value": self.value}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column <= self.value).count()


class Equal(Rule):
    """Expects column values to equal a given value."""
    def __init__(self, value: Any, tolerance: Union[int, float] = 0, raise_exception: bool = False):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.value = value

    @property
    def details(self) -> dict[str, Any]:
        return {"value": self.value}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column != self.value).count()


class NotEqual(Rule):
    """Expects column values to differ from a given value."""
    def __init__(self, value: Any, tolerance: Union[int, float] = 0, raise_exception: bool = False):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.value = value

    @property
    def details(self) -> dict[str, Any]:
        return {"value": self.value}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column == self.value).count()


class Unique(Rule):
    """Expects column values to contain no duplicates."""
    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        duplicate_count = (
            df.groupBy(column)
            .count()
            .filter(F.col("count") > 1)
            .agg(F.sum("count"))
            .collect()[0][0]
        )
        return duplicate_count or 0


class Match(Rule):
    """Expects column values to match a regular expression pattern."""
    def __init__(self, pattern: str, tolerance: Union[int, float] = 0, raise_exception: bool = False):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.pattern = pattern

    @property
    def details(self) -> dict[str, Any]:
        return {"pattern": self.pattern}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(~column.rlike(self.pattern)).count()


class NotMatch(Rule):
    """Expects column values to NOT match a regular expression pattern."""
    def __init__(self, pattern: str, tolerance: Union[int, float] = 0, raise_exception: bool = False):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.pattern = pattern

    @property
    def details(self) -> dict[str, Any]:
        return {"pattern": self.pattern}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        return df.filter(column.rlike(self.pattern)).count()


class Expr(Rule):
    """Expects that column values satisfy a custom filter expression."""
    def __init__(
        self,
        filter_expr: Union[str, DuckSparkColumn],
        tolerance: Union[int, float] = 0,
        raise_exception: bool = False,
    ):
        super().__init__(tolerance=tolerance, raise_exception=raise_exception)
        self.filter_expr = filter_expr

    @property
    def details(self) -> dict[str, Any]:
        expr_repr = (
            self.filter_expr
            if isinstance(self.filter_expr, str)
            else getattr(self.filter_expr, "_jc", None) and str(self.filter_expr)
        )
        return {"filter_expr": expr_repr or "<column expression>"}

    def _count_failures(self, column: DuckSparkColumn, df: DuckSparkDataFrame) -> int:
        expr = (
            self.filter_expr
            if not isinstance(self.filter_expr, str)
            else F.expr(self.filter_expr)
        )
        return df.filter(~expr).count()