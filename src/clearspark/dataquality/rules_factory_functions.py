
from clearspark.dataquality.rules import (
    Rule,
    Null,
    NotNull,
    LessThan,
    MoreThan,
    Equal,
    NotEqual,
    Unique,
    Match,
    NotMatch,
    Expr
)

from clearspark.validation.annotations import DuckSparkColumn

from typing import (
    Union,
    Any
)

from pydantic import (
    ConfigDict, 
    validate_call
)

__all__ = [
    'null',
    'not_null',
    'less_than',
    'more_than',
    'equal',
    'not_equal',
    'unique',
    'match',
    'not_match',
    'expr'
]

@validate_call()
def null(
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to be null.

    Args:
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `Null` rule instance.

    Example:
        >>> dq.null(tolerance=0.05)
    """
    return Null(
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def not_null(
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to be non-null.

    Useful for columns expected to always be filled in (e.g. an `id`
    column).

    Args:
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `NotNull` rule instance.

    Example:
        >>> dq.not_null(raise_exception=True)
    """
    return NotNull(
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def less_than(
    value: Union[int, float],
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to be strictly less than `value`.

    Args:
        value: Threshold; rows with a value at or above this count
            as failures.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `LessThan` rule instance.

    Example:
        >>> dq.less_than(0, tolerance=0.01)
    """
    return LessThan(
        value=value,
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def more_than(
    value: Union[int, float],
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to be strictly greater than `value`.

    Args:
        value: Threshold; rows with a value at or below this count
            as failures.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `MoreThan` rule instance.

    Example:
        >>> dq.more_than(0, tolerance=10)
    """
    return MoreThan(
        value=value,
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def equal(
    value: Any,
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to equal `value`.

    Useful for ruleing that a column holds a constant or expected
    value across all rows.

    Args:
        value: Expected value; rows that differ from it count as
            failures.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `Equal` rule instance.

    Example:
        >>> dq.equal("active", tolerance=0.02)
    """
    return Equal(
        value=value,
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def not_equal(
    value: Any,
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to differ from `value`.

    Useful for catching rows that fell into an unexpected or sentinel
    state (e.g. flagging a default placeholder value).

    Args:
        value: Value that rows should NOT have; rows matching it count
            as failures.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `NotEqual` rule instance.

    Example:
        >>> dq.not_equal("unknown", tolerance=5)
    """
    return NotEqual(
        value=value,
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def expr(
    filter_expr: Union[str, DuckSparkColumn],
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects every row to satisfy a custom filter expression.

    Args:
        filter_expr: SQL expression string or column expression
            describing the rows considered passing (i.e. rows NOT
            matching `filter_expr` count as failures).
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `Expr` rule instance.

    Example:
        >>> dq.expr("amount >= 0", tolerance=0.10)
    """
    return Expr(
        filter_expr=filter_expr,
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def unique(
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to be unique (no duplicates).

    Args:
        tolerance: Maximum number of failing rows (duplicates) allowed
            before the rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `Unique` rule instance.

    Example:
        >>> dq.unique(tolerance=0.01)
    """
    return Unique(
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def match(
    pattern: str,
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to match a regex pattern.

    Useful for enforcing an expected format (e.g. requiring values to
    contain only numeric characters).

    Args:
        pattern: Regular expression; rows NOT matching it count as
            failures.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `Match` rule instance.

    Example:
        >>> dq.match(r"^[0-9]+$", tolerance=2)
    """
    return Match(
        pattern=pattern,
        tolerance=tolerance,
        raise_exception=raise_exception
    )

@validate_call()
def not_match(
    pattern: str,
    tolerance: Union[int, float] = 0,
    raise_exception: bool = False
) -> Rule:
    """Expects the column value to NOT match a regex pattern.

    Useful for detecting forbidden characters or known bad patterns
    (e.g. flagging values containing non-numeric characters).

    Args:
        pattern: Regular expression; rows matching it count as
            failures.
        tolerance: Maximum number of failing rows allowed before the
            rule is considered failed. Can be an absolute count (int) or 
            percentage (float). Defaults to 0.
        raise_exception: Whether to raise an exception immediately when
            the rule fails. Defaults to False.

    Returns:
        A configured `NotMatch` rule instance.

    Example:
        >>> dq.not_match(r"[^0-9]", tolerance=0.005)
    """
    return NotMatch(
        pattern=pattern,
        tolerance=tolerance,
        raise_exception=raise_exception
    )