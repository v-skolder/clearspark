# Function Reference

All public functions are available under `clearspark.functions`:

```python
import clearspark.functions as cf
```

## Index

- [Load / Save](#load--save)
  - [`load_data`](#load_data)
  - [`save_data`](#save_data)
- [Transform](#transform)
  - [`with_categories`](#with_categories)
  - [`with_buckets`](#with_buckets)

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

---

## Transform

Functions for deriving or reshaping columns on an existing DataFrame.

### `with_categories`

```python
with_categories(
    df: DuckSparkDataFrame,
    origin_value_column: Union[str, DuckSparkColumn],
    group_column_nm: str,
    categories: dict[str, list[str]] = Field(min_length=1),
    default: str = "uncategorized",
) -> DataFrame
```

Adds a categorical column based on value-to-label mappings.

For each row, the value in `origin_value_column` is checked against each list of values in `categories`. The first matching label is assigned; rows that match none of the categories receive `default`.

**Parameters**

| Name | Type | Default | Description |
|---|---|---|---|
| `df` | `DuckSparkDataFrame` | — | The PySpark DataFrame to transform. |
| `origin_value_column` | `Union[str, DuckSparkColumn]` | — | Column to categorize, as a column name or `DuckSparkColumn` expression. |
| `group_column_nm` | `str` | — | Name of the new categorical column to create. |
| `categories` | `dict[str, list[str]]` | required, non-empty | Mapping of label -> list of values that belong to that label. |
| `default` | `str` | `"uncategorized"` | Label assigned to rows that match no category. |

**Returns**

`DataFrame` — with the new categorical column added.

**Examples**

```python
# Categorize HTTP status codes into groups
cf.with_categories(
    df,
    origin_value_column="status_code",
    group_column_nm="status_group",
    categories={
        "success": ["200", "201", "204"],
        "client_error": ["400", "404"],
        "server_error": ["500", "502", "503"],
    },
)

# Use a custom default label for unmatched rows
cf.with_categories(
    df,
    origin_value_column="country",
    group_column_nm="region",
    categories={"latam": ["BR", "AR", "CL"]},
    default="other",
)
```

### `with_buckets`

```python
with_buckets(
    df: DuckSparkDataFrame,
    origin_value_column: Union[str, DuckSparkColumn],
    group_column_nm: str,
    buckets: Union[list[int], list[float]] = Field(min_length=1),
    default: str = "missing",
    suffix: Optional[str] = "",
) -> DataFrame
```

Adds a bucketed (binned) column based on numeric ranges.

Sorts `buckets` ascending and assigns each row a label based on which range its value in `origin_value_column` falls into. Ranges are half-open (`[lower, upper)`), so a value equal to a boundary belongs to the bucket that starts at that boundary. Values below the first or above the last boundary still receive a label (open-ended ranges); only null or non-matching values fall back to `default`.

**Parameters**

| Name | Type | Default | Description |
|---|---|---|---|
| `df` | `DuckSparkDataFrame` | — | The PySpark DataFrame to transform. |
| `origin_value_column` | `Union[str, DuckSparkColumn]` | — | Numeric column to bucket, as a column name or `DuckSparkColumn` expression. |
| `group_column_nm` | `str` | — | Name of the new bucketed column to create. |
| `buckets` | `Union[list[int], list[float]]` | required, non-empty | Boundary values defining the ranges. Sorted internally, so order doesn't matter. |
| `default` | `str` | `"missing"` | Label assigned to null or non-matching values. |
| `suffix` | `Optional[str]` | `""` | Text appended to every generated label (e.g. a unit like `"kg"`). |

**Returns**

`DataFrame` — with the new bucketed column added.

**Examples**

```python
# Bucket ages into ranges
cf.with_buckets(
    df,
    origin_value_column="age",
    group_column_nm="age_group",
    buckets=[18, 30, 60],
)
# labels: "00. <18", "01. 18 - 30", "02. 30 - 60", "03. >=60"

# Bucket with a unit suffix
cf.with_buckets(
    df,
    origin_value_column="weight_kg",
    group_column_nm="weight_group",
    buckets=[50, 70, 90],
    suffix="kg",
)
```