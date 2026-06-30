
from clearspark.validation.annotations import (
    DuckSparkColumn, 
    DuckSparkSession,
    DuckSparkDataFrame
)

from typing import (
    Optional, 
    Union
)

from pydantic import (
    ConfigDict, 
    validate_call,
    Field
)

from pyspark.sql import (
    DataFrame, 
    SparkSession
)

import pyspark.sql.functions as F

__all__ = [
    "load_data",
    "save_data",
    "with_categories",
    "with_buckets"
]

# UTILS

def _is_catalog_path(path: str):
    return "/" not in path

# FUNCTIONS

@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def load_data(
    path: str,
    format: str = "delta",
    select_cols: Optional[Union[list[str], list[DuckSparkColumn]]] = None,
    filter_spec: Optional[Union[str, DuckSparkColumn]] = None,
    spark_session: Optional[DuckSparkSession] = None,
) -> DataFrame:
    """Loads a DataFrame from a catalog table or file path.

    Reads data using the given format, optionally selecting specific
    columns and applying a filter. The source is resolved as a catalog
    table if `path` contains no "/", otherwise as a file path.

    Args:
        path: Catalog table name (e.g. "db.table") or file path
            (e.g. "/data/events").
        format: Data source format. Defaults to "delta".
        select_cols: Columns to select, as column names or
            `DuckSparkColumn` expressions. If None, all columns are
            returned.
        filter_spec: Filter condition as a SQL string or a
            `DuckSparkColumn` expression. If None, no filter is applied.
        spark_session: Session to use. If None, the active
            `SparkSession` is used.

    Returns:
        A `DataFrame` with the loaded (and optionally filtered/selected)
        data.

    Raises:
        ValueError: If no active Spark session is found and
            `spark_session` is not provided.

    Example:
        >>> load_data("db.events", select_cols=["id", "ts"])
        >>> load_data("/data/events", format="parquet", filter_spec="ts > 0")
    """

    reader = (spark_session or SparkSession.getActiveSession()).read.format(format)

    df = reader.table(path) if _is_catalog_path(path) else reader.load(path)

    if select_cols is not None:
        df = df.select(select_cols)

    if filter_spec is not None:
        df = df.filter(filter_spec)

    return df

@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def save_data(
        df: DuckSparkDataFrame,
        data_path: str,
        data_format: str = "delta",
        mode: str = "overwrite",
        options: Optional[dict[str, str]] = None,
        partition_by: Optional[list[str]] = None,
) -> None:
    """Saves a DataFrame to a catalog table or file path.

    Args:
        df: The PySpark DataFrame to save.
        data_path: The destination path. If it contains no "/", saves as a catalog table; otherwise, as a file path.
        data_format: The format to save in (e.g., "delta", "parquet"). Defaults to "delta".
        mode: The save mode ("overwrite", "append", "ignore", "error"). Defaults to "overwrite".
        options: Optional dictionary of additional options for the writer.
        partition_by: Optional list of column names to partition by.

    Returns:
        None

    Raises:
        ValueError: If the DataFrame or save mode is invalid.

    Example:
        >>> save_data(df, "db.events")
        >>> save_data(df, "/data/events", data_format="parquet", mode="append")
        >>> save_data(df, "db.events", partition_by=["year", "month"])
    """
    writer = df.write.format(data_format).mode(mode)

    if options:
        writer = writer.options(**options)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if _is_catalog_path(data_path):
        writer.saveAsTable(data_path)
    else:
        writer.save(data_path)

    print(f"Data saved successfully to '{data_path}' in '{data_format}' format with mode '{mode}'.")

@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def with_categories(
    df: DuckSparkDataFrame,
    origin_value_column: Union[str, DuckSparkColumn],
    group_column_nm: str,
    categories: dict[str, list[str]] = Field(min_length=1),
    default: str = "uncategorized"
) -> DataFrame:
    """Adds a categorical column based on value-to-label mappings.

    For each row, the value in `origin_value_column` is checked against
    each list of values in `categories`. The first matching label is
    assigned; rows that match none of the categories receive `default`.

    Args:
        df: The PySpark DataFrame to transform.
        origin_value_column: Column to categorize, as a column name or
            `DuckSparkColumn` expression.
        group_column_nm: Name of the new categorical column to create.
        categories: Mapping of label -> list of values that belong to
            that label. Must not be empty.
        default: Label assigned to rows that match no category.
            Defaults to "uncategorized".

    Returns:
        A `DataFrame` with the new categorical column added.

    Example:
        >>> with_categories(
        ...     df,
        ...     origin_value_column="status_code",
        ...     group_column_nm="status_group",
        ...     categories={
        ...         "success": ["200", "201", "204"],
        ...         "client_error": ["400", "404"],
        ...         "server_error": ["500", "502", "503"],
        ...     },
        ... )
    """
    col = F.col(origin_value_column) if isinstance(origin_value_column, str) else origin_value_column

    expr = None
    for label, values in categories.items():
        condition = col.isin(values)
        expr = F.when(condition, F.lit(label)) if expr is None else expr.when(condition, F.lit(label))

    expr = expr.otherwise(F.lit(default))
    
    return df.withColumn(group_column_nm, expr)

@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def with_buckets(
    df: DuckSparkDataFrame,
    origin_value_column: Union[str, DuckSparkColumn],
    group_column_nm: str,
    buckets: Union[list[int], list[float]] = Field(min_length=1),
    default: str = "missing",
    suffix: Optional[str] = ""
) -> DataFrame:
    """Adds a bucketed (binned) column based on numeric ranges.

    Sorts `buckets` ascending and assigns each row a label based on
    which range its value in `origin_value_column` falls into. Ranges
    are half-open (`[lower, upper)`), so a value equal to a boundary
    belongs to the bucket that starts at that boundary. Values below
    the first or above the last boundary still receive a label
    (open-ended ranges); only non-numeric or null values fall back
    to `default`.

    Args:
        df: The PySpark DataFrame to transform.
        origin_value_column: Numeric column to bucket, as a column
            name or `DuckSparkColumn` expression.
        group_column_nm: Name of the new bucketed column to create.
        buckets: Boundary values defining the ranges. Must not be
            empty; sorted internally, so order doesn't matter.
        default: Label assigned to null or non-matching values.
            Defaults to "missing".
        suffix: Optional text appended to every generated label
            (e.g. a unit like "kg"). Defaults to "".

    Returns:
        A `DataFrame` with the new bucketed column added.

    Example:
        >>> with_buckets(
        ...     df,
        ...     origin_value_column="age",
        ...     group_column_nm="age_group",
        ...     buckets=[18, 30, 60],
        ... )
        # labels: "00. <18", "01. 18 - 30", "02. 30 - 60", "03. >=60"
    """
    col = F.col(origin_value_column) if isinstance(origin_value_column, str) else origin_value_column
    sorted_buckets = sorted(buckets)
    suffix_part = f" {suffix}" if suffix else ""

    expr = F.when(
        col < sorted_buckets[0], 
        F.lit(f"00. <{sorted_buckets[0]}{suffix_part}")
    )

    for i in range(1, len(sorted_buckets)):
        lower, upper = sorted_buckets[i - 1], sorted_buckets[i]
        condition = (col >= lower) & (col < upper)
        
        expr = expr.when(
            condition, 
            F.lit(f"{i:02d}. {lower} - {upper}{suffix_part}")
        )

    last_idx = len(sorted_buckets)
    expr = expr.when(
        col >= sorted_buckets[-1], 
        F.lit(f"{last_idx:02d}. >={sorted_buckets[-1]}{suffix_part}")
    )
    
    expr = expr.otherwise(F.lit(default))

    return df.withColumn(group_column_nm, expr)
