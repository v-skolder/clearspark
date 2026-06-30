===============================================================================
                                 DATA QUALITY
===============================================================================

The `dataquality` module lets you validate a DataFrame against a list of rules 
and get back a structured, queryable report — no manual `filter`/`count` boilerplate.

-------------------------------------------------------------------------------
1. QUICK START EXAMPLE
-------------------------------------------------------------------------------

import clearspark.functions as cf
import clearspark.dataquality as dq

df = cf.load_data("public.x")

engine = dq.Engine(
    stages=[
        {"id": [dq.not_null(), dq.unique()]},
        {"amount": [dq.more_than(0, raise_exception=True)]},
    ]
)

output = engine.verify(df)
print(output)
# 3/4 rules passed (1 failed)


-------------------------------------------------------------------------------
2. HOW IT WORKS
-------------------------------------------------------------------------------

- An `Engine` is configured with `stages`: a list of `{column: [rules]}` dicts.
- Each rule describes the condition you expect the column to satisfy. The rule's 
  name describes what a PASSING row looks like — e.g. `more_than(0)` expects every 
  value to be greater than 0; rows that aren't are flagged as failures.
- `tolerance` lets a rule absorb a number of failures before being reported as 
  failed. ACCEPTS INTEGERS FOR AN ABSOLUTE ROW COUNT OR FLOATS FOR A PERCENTAGE THRESHOLD.
- `raise_exception=True` flags that an exception should be raised if a rule fails 
  beyond its tolerance. Execution of rules continues until the end of the current 
  stage; the exception is deferred and officially raised only after that stage finishes.
- `engine.verify(df)` runs every rule and returns a `VerificationResult` 
  aggregating all outcomes.
- A `HistoryStore` can be used to explicitly persist and query historical 
  validation results through a chosen backend (SQLite, S3, or Catalog).


-------------------------------------------------------------------------------
3. COMPONENT REFERENCE
-------------------------------------------------------------------------------

--- ENGINE ---
dq.Engine(stages: list[dict[str | Column, list[Rule]]])

Orchestrates rules across one or more stages of a DataFrame.

Parameters:
  - stages (list[dict]): List of {column: [rules]} mappings. Column can be a 
                         string name or a column expression (e.g. F.col(...)). 
                         Must contain at least one stage.

Methods:
  - verify(df) -> VerificationResult
    Runs every configured rule against `df` and returns the aggregated result.
    If any rule has `raise_exception=True` and fails beyond tolerance, the error 
    is caught and accumulated, halting further stages only *after* the current 
    stage has completed processing.

Properties:
  - last_result
    Property holding the `VerificationResult` from the most recent `verify()` 
    call (`None` if `verify()` hasn't run yet).


--- VERIFICATIONRESULT ---
Returned by `engine.verify(df)`. Aggregates every individual rule outcome.

Properties / Methods:
  - .total (int): Total number of rules executed.
  - .failed (int): Number of rules that did not pass.
  - .passed (bool): True if every rule passed.
  - .failures (list[RuleResult]): Only the rules that did not pass.
  - .summary() (str): Multi-line, colorized human-readable report displaying 
                      details and error percentages.
  - repr(result) (str): One-line summary, e.g. "7/9 rules passed (2 failed)".

Example Usage:
    output = engine.verify(df)
    output.passed          # False
    output.failed          # 2
    print(output)          # 7/9 rules passed (2 failed)
    print(output.summary())
    # [FAILED] NotNull column='id' (1/9 failed) (11.1% error)
    # [FAILED] MoreThan(value=0) column='amount' (1/9 failed) (11.1% error)


--- RULERESULT ---
One entry per rule executed, found in `VerificationResult.rule_results` / `.failures`.

Fields:
  - rule_name (str): Name of the ruleer (e.g. "MoreThan").
  - column_name (str): Column the rule ran against.
  - total_count (int): Total rows evaluated.
  - failed_count (int): Rows that failed the rule.
  - tolerance (int | float): Absolute rows (int) or percentage (float) allowed.
  - details (dict): Extra parameters of the rule (e.g. {"value": 0}).
  - .passed (bool): True if `failed_count <= calculated_tolerance`.


--- HISTORYSTORE ---
dq.HistoryStore(type: str | None = None, path: str | None = None)

Saves and queries `VerificationResult` history through a persistent backend. 
Verifying a DataFrame never automatically persists it; `save` must be called explicitly.

Parameters:
  - type (str, optional): The backend type ("local", "s3", or "catalog"). If 
                          omitted, it is automatically inferred from the `path` 
                          parameter via regex rules.
  - path (str, optional): The target storage location or identifier.

Type Inference Rules (when type=None):
  - Starts with `s3://` or `s3a://`                   -> Infers "s3"
  - Matches dot-separated schema/table (`layer.table`) -> Infers "catalog"
  - Any other string path or `None`                    -> Infers "local"

Methods:
  - save(output: VerificationResult) -> None
    Persists the verification results to the configured backend.
  - load(rule_name: str | None = None, column_name: str | None = None, 
         stage: int | None = None, passed: bool | None = None, 
         last_n: int | None = None) -> pd.DataFrame
    Loads historical validation runs from the backend as a pandas DataFrame, 
    applying any provided filters and ordering by execution time.


-------------------------------------------------------------------------------
4. RULES REFERENCE
-------------------------------------------------------------------------------

Every rule function takes a common `tolerance` and `raise_exception` parameter:
  - tolerance (int | float, default=0): Max failures allowed. `int` for absolute 
                                        rows, `float` for percentage (0.05 = 5%).
  - raise_exception (bool, default=False): Raise `RuleValidationError` after the 
                                           current stage finishes processing when 
                                           the rule fails beyond tolerance.

Naming convention: each rule's name describes the condition you EXPECT a row 
to satisfy, not the failure condition.

--- null(tolerance=0, raise_exception=False) ---
Expects the column value to be NULL. Flags rows where the value is not null.
Example: dq.null(tolerance=0.01)

--- not_null(tolerance=0, raise_exception=False) ---
Expects the column value to be NON-NULL. Flags rows where the value is null.
Example: dq.not_null(raise_exception=True)

--- less_than(value, tolerance=0, raise_exception=False) ---
Expects the column value to be strictly LESS THAN `value`. Flags rows where 
the value is at or above the threshold.
Args: value (int | float)
Example: dq.less_than(0)

--- more_than(value, tolerance=0, raise_exception=False) ---
Expects the column value to be strictly GREATER THAN `value`. Flags rows where 
the value is at or below the threshold.
Args: value (int | float)
Example: dq.more_than(0)

--- equal(value, tolerance=0, raise_exception=False) ---
Expects the column value to EQUAL `value`.
Args: value (Any)
Example: dq.equal("active")

--- not_equal(value, tolerance=0, raise_exception=False) ---
Expects the column value to DIFFER FROM `value`. Flags sentinel/placeholder values.
Args: value (Any)
Example: dq.not_equal("unknown")

--- unique(tolerance=0, raise_exception=False) ---
Expects the column value to be UNIQUE. Flags duplicated values.
Example: dq.unique(raise_exception=True)

--- match(pattern, tolerance=0, raise_exception=False) ---
Expects the column value to MATCH `pattern` regex.
Args: pattern (str)
Example: dq.match(r"^[0-9]+$")

--- not_match(pattern, tolerance=0, raise_exception=False) ---
Expects the column value to NOT MATCH `pattern` regex.
Args: pattern (str)
Example: dq.not_match(r"[^0-9]")

--- expr(filter_expr, tolerance=0, raise_exception=False) ---
Expects every row to SATISFY a custom filter expression.
Args: filter_expr (str | Column)
Example: dq.expr("amount >= 0")


-------------------------------------------------------------------------------
5. FULL PRODUCTION EXAMPLE
-------------------------------------------------------------------------------

import clearspark.functions as cf
import clearspark.dataquality as dq
import pyspark.sql.functions as F

df = cf.load_data("public.orders")

engine = dq.Engine(
    stages=[
        {"order_id": [dq.not_null(), dq.unique()]},
        {"amount": [dq.more_than(0, raise_exception=True), dq.less_than(50000)]},
        {"status": [dq.not_equal("unknown", tolerance=0.05)]}, 
        {"email": [dq.not_null(tolerance=5)]},                 
        {F.col("country_code"): [dq.match(r"^[A-Z]+$")]},
    ]
)

output = engine.verify(df)

# Initialize HistoryStore (backends inferred automatically via regex)
store_s3   = dq.HistoryStore(path="s3://my-bucket/dq_history")       # Type: s3
store_cat  = dq.HistoryStore(path="observability.orders_history")    # Type: catalog
store_loc  = dq.HistoryStore()                                       # Type: local

# Persist and load historical outcomes
if not output.passed:
    print(output.summary())
    store_s3.save(output)

# Querying historical logs
history_df = store_s3.load(column_name="email", last_n=10)
===============================================================================