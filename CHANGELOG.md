# Changelog

## 1.3.6

### Fixed
- **`Engine.verify()` did not propagate `stage` to `RuleResult`.** Every result was created with the default `stage` of the dataclass (`1`), regardless of how many stages existed in `Engine(stages=[...])`. This silently broke:
  - `VerificationResult.stages` (grouping by stage);
  - `VerificationResult.summary()` (report always showed "Stage 1/N" for everything);
  - the `stage=` filter in `HistoryStore.load()`.

  Now, `Engine.verify()` iterates through stages using `enumerate(..., start=1)` and assigns `rule_result.stage = stage_number` to each result, including results captured in `RuleValidationError` when `raise_exception=True`.

- `Engine.verify()` was catching generic `Exception` to accumulate stage failures, which hid actual bugs (e.g., `TypeError` from a malformed filter) behind the normal "rule failed" flow. Now, only `RuleValidationError` is caught; any other exception propagates immediately.