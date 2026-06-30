
from dataclasses import (
    dataclass, 
    field
)
from datetime import datetime, timezone

import json
import pandas as pd

from typing import Any

__all__ = [
    'RuleResult',
    'VerificationResult', 
    'RuleValidationError'
]

@dataclass
class RuleResult:
    """Result of running a single Rule against a column.

    Attributes:
        rule_name: Name of the Rule that produced this result
            (e.g. "LessThan", "Null").
        column_name: Name of the column the rule was run against.
        total_count: Total number of rows evaluated.
        failed_count: Number of rows that failed the rule.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed.
        details: Extra parameters of the rule (e.g. {"value": 0} for
            `less_than(0)`), included for reporting purposes.
        stage: Index (1-based) of the stage this rule belongs to.

    Example:
        >>> result = RuleResult(
        ...     rule_name="LessThan",
        ...     column_name="amount",
        ...     total_count=1000,
        ...     failed_count=3,
        ...     tolerance=5,
        ...     details={"value": 0},
        ...     stage=1,
        ... )
        >>> result.passed
        True
    """
    rule_name: str
    column_name: str
    total_count: int
    failed_count: int
    tolerance: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    stage: int = 1

    @property
    def passed(self) -> bool:
        """Whether the rule passed, accounting for tolerance.

        Returns:
            True if `failed_count` is within the allowed `tolerance`.

        Example:
            >>> RuleResult("Null", "id", 100, 5, tolerance=10).passed
            True
            >>> RuleResult("Null", "id", 100, 15, tolerance=10).passed
            False
        """
        return self.failed_count <= self.tolerance

    @property
    def error_pct(self) -> float:
        """Percentage of evaluated rows that failed the rule.

        Example:
            >>> RuleResult("Null", "id", 1000, 3, tolerance=0).error_pct
            0.3
        """
        if self.total_count == 0:
            return 0.0
        return self.failed_count / self.total_count * 100

    @property
    def tolerance_display(self) -> str:
        """Human-readable tolerance, percentage for float, row count for int.

        Example:
            >>> RuleResult("Null", "id", 100, 5, tolerance=0.5).tolerance_display
            '50.0%'
            >>> RuleResult("Null", "id", 100, 5, tolerance=5).tolerance_display
            '5'
        """
        if isinstance(self.tolerance, float):
            return f"{self.tolerance * 100:.1f}%"
        return str(self.tolerance)

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        details_str = f" {self.details}" if self.details else ""
        return (
            f"<{self.rule_name} column='{self.column_name}'{details_str} "
            f"{status} ({self.failed_count}/{self.total_count} failed, "
            f"tolerance={self.tolerance})>"
        )


class RuleValidationError(Exception):
    """Raised when a Rule with `raise_exception=True` fails.

    Attributes:
        result: The `RuleResult` that triggered this exception.

    Example:
        >>> raise RuleValidationError(result)
        Traceback (most recent call last):
            ...
        RuleValidationError: rule 'Null' failed on column 'id': ...
    """

    def __init__(self, result: RuleResult):
        self.result = result
        super().__init__(
            f"rule '{result.rule_name}' failed on column "
            f"'{result.column_name}': {result.failed_count}/{result.total_count} "
            f"rows failed (tolerance={result.tolerance})."
        )


class VerificationResult:
    """Aggregated result of running an `Engine.verify()` call.

    Attributes:
        rule_results: List of individual `RuleResult` objects, one
            per rule executed.

    Example:
        >>> output = engine.verify(df)
        >>> output.passed
        False
        >>> output.failed
        2
        >>> print(output)
        7/9 rules passed (2 failed)
    """

    def __init__(self, rule_results: list[RuleResult]):
        self.rule_results = rule_results
        self.executed_at = datetime.now(timezone.utc)
        self.execution_id = self.executed_at.strftime("%Y%m%d%H%M%S%f")

    @property
    def total(self) -> int:
        """Total number of rules executed."""
        return len(self.rule_results)

    @property
    def failed(self) -> int:
        """Number of rules that did not pass."""
        return sum(1 for r in self.rule_results if not r.passed)

    @property
    def passed(self) -> bool:
        """Whether every rule passed."""
        return self.failed == 0

    @property
    def failures(self) -> list[RuleResult]:
        """List of `RuleResult` objects that did not pass.

        Example:
            >>> [f.column_name for f in output.failures]
            ['amount', 'email']
        """
        return [r for r in self.rule_results if not r.passed]

    @property
    def stages(self) -> dict[int, list[RuleResult]]:
        """Rule results grouped by stage, preserving execution order.

        Example:
            >>> output.stages[1]
            [<NotNull column='order_id' ...>, <Unique column='order_id' ...>]
        """
        grouped: dict[int, list[RuleResult]] = {}
        for r in self.rule_results:
            grouped.setdefault(r.stage, []).append(r)
        return grouped

    def summary(self) -> str:
        """Human-readable summary of all rule results, grouped by stage.

        Example:
            >>> print(output.summary())
            Stage 1/4
              [PASSED] NotNull column='order_id' (0/10000 failed, 0.0% error, tolerance=0)
              [PASSED] Unique column='order_id' (0/10000 failed, 0.0% error, tolerance=0)
              [FAILED] MoreThan(value=0) column='amount' (12/10000 failed, 0.1% error, tolerance=0)
              [PASSED] LessThan(value=50000) column='amount' (0/10000 failed, 0.0% error, tolerance=0)
            <BLANKLINE>
            Stage 2/4
              [PASSED] NotEqual(value='unknown') column='status' (430/10000 failed, 4.3% error, tolerance=50.0%)
            <BLANKLINE>
            ------------------------------------------------------------
            7/9 rules passed (2 failed)
        """
        stages = self.stages
        stage_count = len(stages)
        lines = []

        for stage_number, results in stages.items():
            lines.append(f"Stage {stage_number}/{stage_count}")
            for r in results:
                status = "\033[92mPASSED\033[0m" if r.passed else "\033[91mFAILED\033[0m"

                details_str = ""
                if r.details:
                    items = [f"{k}={repr(v)}" for k, v in r.details.items() if v is not None]
                    if items:
                        details_str = f"({', '.join(items)})"

                lines.append(
                    f"  [{status}] {r.rule_name}{details_str} column='{r.column_name}' "
                    f"({r.failed_count}/{r.total_count} failed, {r.error_pct:.1f}% error, "
                    f"tolerance={r.tolerance_display})"
                )
            lines.append("")

        lines.append("-" * 60)
        lines.append(repr(self))
        return "\n".join(lines)

    def to_dataframe(self) -> pd.DataFrame:
        """Flatten all rule results into a pandas DataFrame, one row per rule.

        Every row carries the same `execution_id`, generated from the
        timestamp of when this `VerificationResult` was created, so multiple
        runs can be concatenated and queried as a history table. `tolerance`
        is stored as its `tolerance_display` string (e.g. "50.0%" or "5") and
        `details` is serialized to JSON for portability across storage formats.

        Example:
            >>> df = output.to_dataframe()
            >>> df.columns.tolist()
            ['execution_id', 'executed_at', 'stage', 'rule_name', 'column_name',
             'total_count', 'failed_count', 'tolerance', 'error_pct', 'passed', 'details']
        """
        return pd.DataFrame([
            {
                "execution_id": self.execution_id,
                "executed_at": self.executed_at,
                "stage": r.stage,
                "rule_name": r.rule_name,
                "column_name": r.column_name,
                "total_count": r.total_count,
                "failed_count": r.failed_count,
                "tolerance": r.tolerance_display,
                "error_pct": r.error_pct,
                "passed": r.passed,
                "details": json.dumps(r.details),
            }
            for r in self.rule_results
        ])

    def __repr__(self) -> str:
        passed_count = self.total - self.failed
        return f"{passed_count}/{self.total} rules passed ({self.failed} failed)"