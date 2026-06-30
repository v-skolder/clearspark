
from clearspark.validation.annotations import (
    DuckSparkColumn,
    DuckSparkDataFrame
)

from clearspark.dataquality.rules import Rule
from clearspark.dataquality.result import VerificationResult
from clearspark.dataquality.result import RuleValidationError

from typing import Union

from pydantic import (
    ConfigDict,
    validate_call,
    Field
)

import pyspark.sql.functions as F

__all__ = [
    'Engine'
]

class Engine:
    """Orchestrates data quality rules across stages of a DataFrame.

    A `stage` is a dict mapping a column (name or column expression)
    to a list of `Rule` instances to run against it. Stages are
    executed in order, and all results are collected into a single
    `VerificationResult`.

    Attributes:
        stages: List of `{column: [Rule, ...]}` dicts to execute.
        last_result: The `VerificationResult` from the most recent
            `verify()` call, or `None` if `verify()` has not run yet.

    Example:
        >>> engine = Engine(
        ...     stages=[
        ...         {'id': [dq.not_null(), dq.unique()]},
        ...         {'amount': [dq.less_than(0, raise_exception=True)]},
        ...     ]
        ... )
        >>> output = engine.verify(df)
        >>> print(output)
        2/3 rules passed (1 failed)
    """

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(
        self,
        stages: list[dict[Union[str, DuckSparkColumn], list[Rule]]] = Field(min_length=1)
    ):
        self._stages = stages
        self._last_result: Union[VerificationResult, None] = None

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def verify(
        self,
        df: DuckSparkDataFrame
    ) -> VerificationResult:
        """Runs every configured rule against the given DataFrame.

        Rules are executed sequentially. However, if any rule has 
        `raise_exception=True` and fails beyond its tolerance, the 
        exception is caught and accumulated. The validation will only 
        be interrupted and the exception raised *after* the current 
        stage is completely processed.

        Args:
            df: DataFrame to validate.

        Returns:
            A `VerificationResult` aggregating every `RuleResult`
            produced by the configured rules.

        Raises:
            Exception: The first encountered rule exception from the stage 
                that failed, raised only after the stage concludes.
        """
        total_count = df.count()
        results = []

        for stage_number, stage in enumerate(self._stages, start=1):
            stage_exceptions = []
            for column_name, rules in stage.items():
                
                resolved_col = (
                    column_name if not isinstance(column_name, str) else F.col(column_name)
                )
                
                for rule in rules:
                    try:
                        rule_result = rule.verify(
                            column=resolved_col,
                            column_name=str(column_name),
                            total_count=total_count,
                            df=df,
                        )
                        rule_result.stage = stage_number
                        results.append(rule_result)
                    except RuleValidationError as e:
                        e.result.stage = stage_number
                        stage_exceptions.append(e)

            if stage_exceptions:
                raise stage_exceptions[0]

        result = VerificationResult(results)
        self._last_result = result
        return result

    @property
    def last_result(self) -> Union[VerificationResult, None]:
        """The `VerificationResult` from the most recent `verify()` call.

        Returns:
            The last `VerificationResult`, or `None` if `verify()`
            has not been called yet on this `Engine` instance.
        """
        return self._last_result