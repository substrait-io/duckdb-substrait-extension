# Release notes

## Unreleased

### Behavior changes

- **Breaking change:** The Substrait consumer now rejects plans if an aggregate or window call omits `phase` or sets it to `AGGREGATION_PHASE_UNSPECIFIED`. This affects older DuckDB exports and DataFusion plans that leave the phase unspecified. Regenerate saved DuckDB plans from SQL with the updated extension. For final results, use `INITIAL_TO_RESULT` on raw rows or `INTERMEDIATE_TO_RESULT` to merge partial non-distinct `COUNT` values. See [Aggregation phases](README.md#aggregation-phases) for the full enum names and examples.
