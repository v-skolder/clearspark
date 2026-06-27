
from clearspark.annotations import (
    DuckSparkColumn, 
    DuckSparkSession,
    DuckSparkDataFrame
)

from typing import (
    Optional, 
    Union,
    Any
)

from pydantic import (
    ConfigDict, 
    validate_call
)

from pyspark.sql import (
    DataFrame, 
    SparkSession
)

import pyspark.sql.functions as F

__all__ = [
    "load_data",
    "save_data",
    "with_categories"
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

def with_categories(
    df: DuckSparkDataFrame,
    origin_value_column: Union[str, DuckSparkColumn],
    new_column_nm: str,
    categories: dict[str, list[str]],
    default: str = "uncategorized"
) -> DataFrame:

    expr = None
    for label, values in categories.items():
        condition = \
            (F.col(origin_value_column) if isinstance(origin_value_column, str) else origin_value_column) \
            .isin(values)
        
        if expr is None:
            expr = F.when(condition, F.lit(label))

        else:
            expr = expr.when(condition, F.lit(label))

    retut