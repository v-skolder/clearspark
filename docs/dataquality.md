# Data Quality

The `clearspark.dataquality` module provides a declarative framework for
validating Spark DataFrames using reusable validation rules.

## Overview

Instead of manually writing multiple `filter()`, `count()`, and
assertion statements, define validation rules and execute them through a
validation engine.

### Validation Flow

``` text
DataFrame
    │
    ▼
 Engine
    │
    ▼
 Stages
    │
    ▼
 Rules
    │
    ▼
VerificationResult
    ├── Summary
    ├── Failures
    └── HistoryStore
```

## Quick Start

``` python
import clearspark.functions as cf
import clearspark.dataquality as dq

df = cf.load_data("public.orders")

engine = dq.Engine(
    stages=[
        {"id": [dq.not_null(), dq.unique()]},
        {"amount": [dq.more_than(0)]},
    ]
)

result = engine.verify(df)
print(result)
```

## Core Concepts

### Engine

`Engine` orchestrates one or more validation stages.

Each stage is executed sequentially. If a rule has
`raise_exception=True`, execution continues until the current stage
finishes. The exception is raised only after that stage completes.

### Rules

Each rule represents the expected state of a column.

Example:

``` python
dq.more_than(0)
```

expects every value to be strictly greater than zero.

### Tolerance

Rules support two tolerance modes.

-   Integer → maximum failed rows.
-   Float → maximum percentage of failed rows.

Examples:

``` python
dq.not_null(tolerance=5)
dq.not_null(tolerance=0.05)
```

### HistoryStore

Validation history is persisted only when `save()` is explicitly called.

Backends:

-   Local
-   S3
-   Catalog

## API Reference

### Engine

``` python
dq.Engine(stages: list[dict[str | Column, list[Rule]]])
```

Methods:

-   `verify(df)` → returns `VerificationResult`.

Properties:

-   `last_result`

### VerificationResult

Properties:

-   `total`
-   `failed`
-   `passed`
-   `failures`

Methods:

-   `summary()`

### RuleResult

Contains:

-   Rule name
-   Column
-   Total rows
-   Failed rows
-   Tolerance
-   Details
-   Pass status

### HistoryStore

``` python
dq.HistoryStore(type=None, path=None)
```

Methods:

-   `save()`
-   `load()`

## Available Rules

  Rule                   Description
  ---------------------- ----------------------------------
  `null()`               Value must be NULL
  `not_null()`           Value cannot be NULL
  `equal(value)`         Value must equal parameter
  `not_equal(value)`     Value must differ from parameter
  `less_than(value)`     Value must be smaller
  `more_than(value)`     Value must be greater
  `unique()`             Values must be unique
  `match(pattern)`       Regex must match
  `not_match(pattern)`   Regex must not match
  `expr(expression)`     Custom Spark expression

All rules accept:

-   `tolerance`
-   `raise_exception`

## Production Example

``` python
import clearspark.functions as cf
import clearspark.dataquality as dq
import pyspark.sql.functions as F

df = cf.load_data("public.orders")

engine = dq.Engine(
    stages=[
        {"order_id": [dq.not_null(), dq.unique()]},
        {"amount": [dq.more_than(0, raise_exception=True), dq.less_than(50000)]},
        {"status": [dq.not_equal("unknown", tolerance=0.05)]},
        {F.col("country_code"): [dq.match(r"^[A-Z]+$")]},
    ]
)

result = engine.verify(df)

store = dq.HistoryStore(path="s3://my-bucket/dq_history")

if not result.passed:
    print(result.summary())
    store.save(result)
```

## Best Practices

-   Group related validations into stages.
-   Use `raise_exception=True` only for critical validations.
-   Prefer percentage tolerance for large datasets.
-   Persist validation history for observability.
-   Keep rules declarative and reusable.
