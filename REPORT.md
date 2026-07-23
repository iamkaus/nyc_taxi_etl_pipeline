# Project Report — NYC Yellow Taxi ETL Pipeline

This report documents the data quality investigation carried out while building the silver layer, and the reasoning behind each cleaning/flagging decision. The goal throughout was to trace anomalies to a root cause before deciding how to handle them, rather than applying blanket cleaning rules.

## 1. Schema enforcement at ingestion

Bronze ingestion uses an explicit `StructType` (matching the current NYC TLC data dictionary) combined with `FAILFAST` read mode, rather than Spark's default schema inference with permissive error handling. This was a deliberate choice: inference-based reads can silently coerce mismatched types or swallow malformed records into a `_corrupt_record` column, which would let bad data pass through undetected. All 5 monthly files passed `FAILFAST` cleanly, confirming they conform to the documented schema.

## 2. The "duplicate rows" false alarm

An initial check using `countDistinct(*all_columns)` suggested ~4.8M of the 18.99M rows (~25%) were exact duplicates (`n_unique` came out lower than `n_rows`). Before acting on this, the finding was verified directly:

```python
df.groupBy(*all_columns).count().filter("count > 1")
```

This returned **zero** true duplicate rows. The discrepancy was traced to how `countDistinct()` handles nulls across multiple columns — it does not reliably count "unique full rows" when several of the specified columns contain nulls, and undercounted as a result. `groupBy(...).count()` is the reliable method for this check, since it treats null as a normal groupable value rather than mishandling it. This distinction is documented here since it's a genuine pitfall worth avoiding in future work.

## 3. The `payment_type = 0` pattern

Investigating the same null counts that caused the false duplicate alarm led to a real finding: five columns — `passenger_count`, `ratecode_id`, `store_and_fwd_flag`, `congestion_surcharge`, and `airport_fee` — were null in exactly the same 4,812,280 rows, confirmed via a direct co-occurrence check (`.filter(all five isNull())`).

This pointed to a single shared cause rather than five independent data quality issues. Two hypotheses were tested:

- **Vendor-specific data feed gap.** Ruled out — the null rows span three different vendors (1, 2, 6), not one.
- **`payment_type = 0`.** Confirmed — all 4,812,280 null rows have `payment_type = 0` exactly, with no exceptions.

`payment_type = 0` is not part of TLC's standard documented code list (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip). It most likely represents either an undocumented "Flex Fare" designation or a systematic gap in metadata capture for a subset of trips — this remains unconfirmed against an authoritative source and is stated here as an open question rather than settled fact.

**Handling:** rather than imputing values for the 5 affected columns (there's no reliable basis for what the "correct" value would have been) or dropping ~25% of the dataset (which would discard real trip data unnecessarily), silver adds a boolean `is_incomplete_record` flag (`payment_type == 0`). This preserves the data and makes the ambiguity explicit and queryable, rather than silently resolving it either way.

## 4. Voided and corrected transactions

A separate, unrelated pattern was found in `improvement_surcharge` and `total_amount`: 117,227 rows had `improvement_surcharge < 0`, and in every case, all other financial columns (fare, tax, tips, tolls) were also negative or zero — consistent with a voided or corrected transaction. Direct inspection (via SQL, limited row samples) confirmed a related ~1,100-row variant where the surcharge floors at 0 instead of going negative, while other fields still show the same negative/zero pattern.

Verification confirmed `improvement_surcharge < 0` always implies `total_amount < 0` (zero counterexamples), meaning the surcharge-negative rows are a subset of the total_amount-negative rows. Filtering on `total_amount >= 0` alone is therefore sufficient to remove the full voided-transaction pattern (~118,327 rows total) — no compound filter condition needed.

## 5. Corrupted pickup timestamps

While validating monthly/weekly aggregates, a handful of trips surfaced with pickup timestamps in 2001, 2008, and 2009 — clearly outside the dataset's actual January–May 2026 scope. These are treated as meter/system clock errors, a known category of issue in TLC data, and are filtered out (`tpep_pickup_datetime` constrained to the dataset's real date range). Post-filter, `MIN`/`MAX` on the pickup timestamp column was verified to fall exactly within `2026-01-01` to `2026-05-31`, confirming no further leakage.

## 6. The `improvement_surcharge = 2.5` anomaly

A single row (out of 18.99M) was found with `improvement_surcharge = 2.5` — not a documented value for this field. Before deciding how to handle it, its actual prevalence was checked directly rather than assumed from a plausible-sounding theory (a "column misalignment" hypothesis, where 2.5 could be a misplaced congestion surcharge or flag-drop fee). Since it affects exactly one row, and that row is already flagged via `is_incomplete_record` (it also has `payment_type = 0`), no separate flag column was added — it would add schema complexity for a single-row anomaly already covered by an existing signal.

## 7. Vendor distribution

Trip volume by vendor: Vendor 2 — 14,930,377 (~78.6%), Vendor 1 — 3,679,227 (~19.4%), Vendor 7 — 234,756 (~1.2%), Vendor 6 — 36,577 (~0.2%). This heavy skew toward vendors 1 and 2 is consistent with the real-world structure of NYC's taxi metering ecosystem, where a small number of technology providers power the large majority of the fleet — daily trip counts exceeding 120,000 for vendor 2 alone are architecturally plausible at this scale rather than anomalous.

## Summary of silver-layer transformations

| Issue | Rows affected | Action |
|---|---|---|
| Voided/corrected transactions | ~118,327 | Dropped (`total_amount >= 0`) |
| Corrupted pickup timestamps (2001/2008/2009) | small number | Dropped (date range filter) |
| `payment_type = 0` / null metadata pattern | 4,812,280 | Flagged (`is_incomplete_record`), not dropped or imputed |
| `improvement_surcharge = 2.5` anomaly | 1 | Left as-is, already covered by `is_incomplete_record` |
| Exact duplicate rows | 0 (confirmed) | No action needed |

## Guiding principle

Where the correct handling of anomalous or missing data was genuinely uncertain, the pipeline favors making the uncertainty visible and queryable over resolving it with an unverified assumption. Dropping or imputing based on a guess would make the silver layer *look* cleaner while quietly encoding a decision that might be wrong; a flag lets downstream consumers make that call with full information.
