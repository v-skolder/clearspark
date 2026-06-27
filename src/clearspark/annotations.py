
from typing import (
    Annotated, 
    Any, 
    Optional
)

from pydantic import BeforeValidator

__all__ = ["DuckSparkSession", "DuckSparkDataFrame", "DuckSparkColumn"]

# UTILS

def _hasattrs(v: Any, required_attr: list[str]) -> bool:
    return all(hasattr(v, attr) for attr in required_attr)


# VALIDATE FUNCTIONS

def validate_spark_session(v: Any) -> Any:
    required_attr = ["read", "createDataFrame", "table", "catalog"]

    if not _hasattrs(v, required_attr) and v is not None:
        raise ValueError(f"Invalid SparkSession-like object: {type(v).__name__}")

    return v


def validate_spark_dataframe(v: Any) -> Any:
    required_attr = ["select", "columns", "filter", "groupBy", "withColumn"]

    if not _hasattrs(v, required_attr):
        raise ValueError(f"Invalid Dataframe-like object: {type(v).__name__}")


def validate_spark_column(v: Any) -> Any:
    required_attr = ["alias", "cast", "desc", "asc"]

    if not _hasattrs(v, required_attr):
        raise ValueError(f"Invalid Column-like object: {type(v).__name__}")

# ANNOTATIONS

DuckSparkSession = Optional[Annotated[Any, BeforeValidator(validate_spark_session)]]

DuckSparkDataFrame = Annotated[Any, BeforeValidator(validate_spark_dataframe)]

DuckSparkColumn = Annotated[Any, BeforeValidator(validate_spark_column)]
