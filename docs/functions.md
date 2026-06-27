# Function Reference

All public functions are available under `clearspark.functions`:

```python
import clearspark.functions as cf
```

## Index

- [Load / Save](#load--save)
  - [`load_data`](#load_data)
  - [`save_data`](#save_data)

---

## Load / Save

Functions for reading and writing DataFrames from/to catalog tables or file paths.

### `load_data`

```python
load_data(
    path: str,
    format: str = "delta",
    select_cols: Optional[Union[list[str], list[DuckSparkColumn]]] = None,
    filter_spec: Optional[Union[str, DuckSparkColumn]] = None,
    spark_session: Optional[DuckSparkSession] = None,
) -> DataFrame
```

Loads a DataFrame from a catalog table or file path.

Reads data using the given format, optionally selecting specific columns and applying a filter. The source is resolved as a catalog table if `path` contains no `/`, otherwise as a file path.

**Parameters**

| Name | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | — | Catalog table name (e.g. `"db.table"`) or file path (e.g. `"/data/events"`). |
| `format` | `str` | `"delta"` | Data source format. |
| `select_cols` | `Optional[Union[list[str], list[DuckSparkColumn]]]` | `None` | Columns to select, as column names or `DuckSparkColumn` expressions. If `None`, all columns are returned. |
| `filter_spec` | `Optional[Union[str, DuckSparkColumn]]` | `None` | Filter condition as a SQL string or a `DuckSparkColumn` expression. If `None`, no filter is applied. |
| `spark_session` | `Optional[DuckSparkSession]` | `None` | Session to use. If `None`, the active `SparkSession` is used. |

**Returns**

`DataFrame` — the loaded (and optionally filtered/selected) data.

**Raises**

`ValueError` — if no active Spark session is found and `spark_session` is not provided.

**Examples**

```python
# Load an entire catalog table
cf.load_data("db.events")

# Load a file path, selecting specific columns
cf.load_data("/data/events", format="parquet", select_cols=["id", "ts"])

# Load with a filter applied
cf.load_data("db.events", filter_spec="ts > 0")
```

### `save_data`

```python
save_data(
    df: DuckSparkDataFrame,
    data_path: str,
    data_format: str = "delta",
    mode: str = "overwrite",
    options: Optional[dict[str, str]] = None,
    partition_by: Optional[list[str]] = None,
) -> None
```

Saves a DataFrame to a catalog table or file path.

The destination is resolved as a catalog table if `data_path` contains no `/`, otherwise as a file path.

**Parameters**

| Name | Type | Default | Description |
|---|---|---|---|
| `df` | `DuckSparkDataFrame` | — | The PySpark DataFrame to save. |
| `data_path` | `str` | — | The destination path. If it contains no `/`, saves as a catalog table; otherwise, as a file path. |
| `data_format` | `str` | `"delta"` | The format to save in (e.g. `"delta"`, `"parquet"`). |
| `mode` | `str` | `"overwrite"` | The save mode (`"overwrite"`, `"append"`, `"ignore"`, `"error"`). |
| `options` | `Optional[dict[str, str]]` | `None` | Additional options for the writer. |
| `partition_by` | `Optional[list[str]]` | `None` | Column names to partition by. |

**Returns**

`None`

**Raises**

`ValueError` — if the DataFrame or save mode is invalid.

**Examples**

```python
# Save to a catalog table
cf.save_data(df, "db.events")

# Save to a file path in a different format and mode
cf.save_data(df, "/data/events", data_format="parquet", mode="append")

# Save partitioned by columns
cf.save_data(df, "db.events", partition_by=["year", "month"])
```