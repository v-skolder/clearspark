
from abc import (
    ABC, 
    abstractmethod
)

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from clearspark.dataquality.result import VerificationResult

__all__ = [
    'HistoryStore',
    'SQLiteBackend',
    'S3Backend',
    'CatalogBackend',
]


class HistoryBackend(ABC):
    """Base interface every persistence backend must implement.

    A backend only knows how to write and read a flat `pd.DataFrame`
    shaped like `VerificationResult.to_dataframe()`. Filtering and
    orchestration live in `HistoryStore`.
    """

    @abstractmethod
    def write(self, df: pd.DataFrame) -> None:
        """Append `df` to the underlying storage."""

    @abstractmethod
    def read(self) -> pd.DataFrame:
        """Return the full history currently in storage."""


@dataclass
class SQLiteBackend(HistoryBackend):
    """Persists history to a local SQLite database.

    By default, the database file is created next to wherever the
    process is running (e.g. the notebook's working directory), so no
    external infrastructure is required to get started.

    Attributes:
        path: Path to the `.db` file. Defaults to `dq_history.db` in
            the current working directory.
        table: Name of the table history is written to.

    Example:
        >>> backend = SQLiteBackend()
        >>> store = HistoryStore(backend)
    """
    path: str = "dq_history.db"
    table: str = "dq_history"

    def write(self, df: pd.DataFrame) -> None:
        import sqlite3

        with sqlite3.connect(self.path) as conn:
            df.to_sql(self.table, conn, if_exists="append", index=False)

    def read(self) -> pd.DataFrame:
        import sqlite3

        if not Path(self.path).exists():
            return pd.DataFrame()

        with sqlite3.connect(self.path) as conn:
            try:
                return pd.read_sql(f"SELECT * FROM {self.table}", conn)
            except pd.errors.DatabaseError:
                return pd.DataFrame()


@dataclass
class S3Backend(HistoryBackend):
    """Persists history to S3 as one Parquet file per execution.

    Requires `s3fs` to be installed. Each call to `write` adds a new
    object under `path`, named after the run's `execution_id`, so
    writes never overwrite previous history.

    Attributes:
        path: S3 prefix to write to, e.g. "s3://bucket/dq_history".

    Example:
        >>> backend = S3Backend(path="s3://my-bucket/dq_history")
        >>> store = HistoryStore(backend)
    """
    path: str

    def write(self, df: pd.DataFrame) -> None:
        execution_id = df["execution_id"].iloc[0]
        df.to_parquet(f"{self.path.rstrip('/')}/{execution_id}.parquet", index=False)

    def read(self) -> pd.DataFrame:
        import s3fs

        fs = s3fs.S3FileSystem()
        prefix = self.path.rstrip("/")
        files = fs.glob(f"{prefix}/*.parquet")

        if not files:
            return pd.DataFrame()

        return pd.concat(
            (pd.read_parquet(f"s3://{f}") for f in files),
            ignore_index=True,
        )


@dataclass
class CatalogBackend(HistoryBackend):
    """Persists history to a table in the Spark catalog (e.g. Hive, Delta).

    Requires a running `SparkSession`. Useful when the rest of the
    pipeline already lives in the lakehouse and history should be
    queryable alongside other tables.

    Attributes:
        table: Fully qualified table name, e.g. "default.dq_history".

    Example:
        >>> backend = CatalogBackend(table="observability.dq_history")
        >>> store = HistoryStore(backend)
    """
    table: str

    def write(self, df: pd.DataFrame) -> None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        spark.createDataFrame(df).write.mode("append").saveAsTable(self.table)

    def read(self) -> pd.DataFrame:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        if not spark.catalog.tableExists(self.table):
            return pd.DataFrame()

        return spark.table(self.table).toPandas()

class HistoryStore:
    """Saves and queries `VerificationResult` history through a backend.

    Attributes:
        backend: The `HistoryBackend` resolved from `type` and `path`.

    Example:
        >>> store = dq.HistoryStore() # Infere "local"
        >>> store = dq.HistoryStore(path="s3://bucket/dq_history") # Infere "s3"
        >>> store = dq.HistoryStore(path="observability.dq_history") # Infere "catalog"
    """

    _BACKENDS: dict[str, type[HistoryBackend]] = {
        "local": SQLiteBackend,
        "s3": S3Backend,
        "catalog": CatalogBackend,
    }

    def __init__(self, type: str | None = None, path: str | None = None):
        # Se o tipo não for especificado, infere a partir do path usando Regex
        if type is None:
            type = self._infer_type(path)

        if type not in self._BACKENDS:
            raise ValueError(
                f"unknown history store type '{type}', expected one of "
                f"{list(self._BACKENDS)}"
            )

        if type == "local":
            self.backend = SQLiteBackend(path=path) if path else SQLiteBackend()
        elif type == "s3":
            self.backend = S3Backend(path=path)
        else:
            self.backend = CatalogBackend(table=path)

    def _infer_type(self, path: str | None) -> str:
        """Auxiliary method to infer backend type using regex."""
        if not path:
            return "local"
 
        if re.match(r"^s3a?://", path):
            return "s3"
        
        if re.match(r"^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$", path):
            return "catalog"
            
        return "local"

    def save(self, output: VerificationResult) -> None:
        """Persist a `VerificationResult` through the configured backend."""
        self.backend.write(output.to_dataframe())

    def load(
        self,
        rule_name: str | None = None,
        column_name: str | None = None,
        stage: int | None = None,
        passed: bool | None = None,
        last_n: int | None = None,
    ) -> pd.DataFrame:
        """Load history from the backend, optionally filtered."""
        df = self.backend.read()

        if df.empty:
            return df

        if rule_name is not None:
            df = df[df["rule_name"] == rule_name]
        if column_name is not None:
            df = df[df["column_name"] == column_name]
        if stage is not None:
            df = df[df["stage"] == stage]
        if passed is not None:
            df = df[df["passed"] == passed]

        df = df.sort_values("executed_at", ascending=False)

        if last_n is not None:
            recent_executions = df["execution_id"].drop_duplicates().head(last_n)
            df = df[df["execution_id"].isin(recent_executions)]

        return df.reset_index(drop=True)