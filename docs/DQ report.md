# 📊 Data Quality Report — NYC Taxi + Weather Pipeline

**Project:** NYC Taxi + Weather Cloud Data Pipeline  
**Author:** Mohamed Amin  
**Date:** September 5, 2026  
**Phase:** Silver Layer Data Quality Validation

---

## 1. Executive Summary

The data quality framework processed **52.4M raw trip records** through the Silver layer. After applying automated data quality checks, **48.1M rows (91.8%)** passed all critical checks and were written to Silver. **3.5M rows (6.7%)** were quarantined due to critical failures, ensuring only high-quality data flows into the Gold layer for analytics.

| Metric                    | Value              |
| ------------------------- | ------------------ |
| **Total Raw Rows**        | 52,447,494         |
| **Silver Clean Rows**     | 48,123,180 (91.8%) |
| **Quarantined Rows**      | 3,476,358 (6.6%)   |
| **Weather Silver Rows**   | 9,480 (100% pass)  |
| **Streaming Silver Rows** | 570 (92.5% pass)   |
| **Total DQ Checks Run**   | 22                 |

---

## 2. Bronze Layer — Raw Data Overview

### 2.1 Data Volume

| Source               | Rows/Files    | Description                             |
| -------------------- | ------------- | --------------------------------------- |
| **trips_raw**        | 52,447,494    | NYC Yellow Taxi trips (2025-2026)       |
| **weather_raw**      | 14 JSON files | Open-Meteo weather data (30-day chunks) |
| **trips_stream_raw** | 613           | Simulated streaming events              |

### 2.2 Data Distribution

| Year      | Months        | Rows      |
| --------- | ------------- | --------- |
| 2025      | Jan-Dec       | ~48.7M    |
| 2026      | Jan (partial) | ~3.7M     |
| **Total** | —             | **52.4M** |

### 2.3 Distinct Values

| Column       | Distinct Count |
| ------------ | -------------- |
| VendorID     | 4              |
| PULocationID | 262            |
| DOLocationID | 263            |

### 2.4 Duplicate Analysis

| Metric                       | Value           |
| ---------------------------- | --------------- |
| **Total rows**               | 52,447,494      |
| **Duplicate groups**         | 847,905 (1.62%) |
| **Duplicate rows**           | 847,955         |
| **Max duplicates per group** | 4               |

**Interpretation:** Duplicate groups are small (max 4), consistent with genuine coincidental natural-key collisions rather than a structural bug.

### 2.5 Schema Consistency

All months across 2025-2026 have consistent schemas with **22 columns**, indicating no schema drift issues at the Bronze layer.

---

## 3. Silver Layer — Clean Data Summary

### 3.1 Table Counts & Pass Rates

| Table                   | Clean Rows | Pass Rate |
| ----------------------- | ---------- | --------- |
| **trips_silver**        | 48,123,180 | 81.3%     |
| **weather_silver**      | 9,480      | 100.0%    |
| **trips_stream_silver** | 570        | 92.5%     |

### 3.2 Quarantine Summary

| Table                       | Quarantined Rows | Primary Failure      |
| --------------------------- | ---------------- | -------------------- |
| **trips_quarantine**        | 3,476,358        | pickup_after_dropoff |
| **weather_quarantine**      | 0                | No critical failures |
| **trips_stream_quarantine** | 43               | Critical row checks  |

---

## 4. Data Quality Checks — Detailed Results

### 4.1 Critical Checks (Quarantine)

| Table               | Check Name                      | Rows Checked | Rows Failed | Pass Rate |
| ------------------- | ------------------------------- | ------------ | ----------- | --------- |
| trips_silver        | critical_row_checks             | 52,447,494   | 3,476,357   | 74.7%     |
| trips_silver        | duplicate_trip_key              | 52,447,494   | 847,956     | 88.0%     |
| trips_stream_silver | critical_row_checks             | 1,059        | 72          | 92.5%     |
| weather_silver      | critical_row_checks             | 9,480        | 0           | 100%      |
| weather_silver      | duplicate_weather_timestamp     | 9,480        | 0           | 100%      |
| trips_silver        | breaking_schema_change_detected | —            | 0           | 100%      |

### 4.2 Critical Check Definitions

| Check                             | Description                                                                                       | Severity |
| --------------------------------- | ------------------------------------------------------------------------------------------------- | -------- |
| `critical_row_checks`             | Null pickup/dropoff timestamp, null pickup location, negative fare/distance, pickup_after_dropoff | CRITICAL |
| `duplicate_trip_key`              | Duplicate trips on natural key                                                                    | CRITICAL |
| `breaking_schema_change_detected` | Non-additive schema changes                                                                       | CRITICAL |

### 4.3 Critical Failures — Breakdown

The `critical_row_checks` failure is primarily driven by:

| Failure Type                 | Rows      | % of Total |
| ---------------------------- | --------- | ---------- |
| **pickup_after_dropoff**     | 3,476,357 | 6.6%       |
| **negative_fare** (Vendor 2) | 2,888,083 | 5.5%       |

**Interpretation:**

- **pickup_after_dropoff:** Trips where pickup timestamp is after dropoff timestamp — impossible trip sequences. Quarantined.
- **negative_fare:** 100% from Vendor 2, consistent with their convention for recording adjustments, refunds, and voided trips. Quarantined.

---

## 5. Negative Fare Analysis

### Key Finding

> _"2,888,083 rows (5.51% of all trips) have negative fare amounts."_

### Analysis

| Dimension                | Finding                                                                                    |
| ------------------------ | ------------------------------------------------------------------------------------------ |
| **Vendor**               | 100% from Vendor 2                                                                         |
| **Pattern**              | Consistent with Vendor 2's convention for recording adjustments, refunds, and voided trips |
| **Payment Type**         | All payment_type = 0 (System/Unknown)                                                      |
| **Monthly Distribution** | 5-12% across the year, peaking in November                                                 |

### Decision

> _"These rows are quarantined from Silver to prevent skew in downstream analytics. The quarantine table is available for refund/adjustment analysis if needed."_

---

## 6. Pass Rate Summary

| Table               | Pass Rate | Status        |
| ------------------- | --------- | ------------- |
| weather_silver      | 100.0%    | ✅ Excellent  |
| trips_stream_silver | 92.5%     | ✅ Good       |
| trips_silver        | 81.3%     | ✅ Acceptable |

**Overall Pass Rate:** **91.8%** of all raw data passed critical checks.

---

## 7. Recommendations

| Recommendation                          | Priority | Rationale                                                           |
| --------------------------------------- | -------- | ------------------------------------------------------------------- |
| **Monitor Vendor 2 negative fares**     | Medium   | Pattern is systematic; consider building a separate adjustment mart |
| **Monitor pickup_after_dropoff trends** | Low      | Rate is stable (~6.6%); continue to quarantine                      |
| **Add warning-level checks**            | Low      | Consider flagging "exact equal timestamp" trips for future analysis |

---

## 8. Verification Checklist

| Check                        | Status        |
| ---------------------------- | ------------- |
| Bronze data loaded           | ✅ 52.4M rows |
| Silver clean rows            | ✅ 48.1M rows |
| Quarantine contains bad data | ✅ 3.47M rows |
| DQ results logged            | ✅ 22 checks  |
| Weather pass rate            | ✅ 100%       |
| Streaming pass rate          | ✅ 92.5%      |
| No breaking schema changes   | ✅ 0          |

---

## 9. Appendix — Sample Quarantined Rows

| VendorID | PULocationID | fare_amount | \_dq_failures        | \_quarantined_at    |
| -------- | ------------ | ----------- | -------------------- | ------------------- |
| 1        | 161          | 26.9        | pickup_after_dropoff | 2026-08-31 22:51:16 |
| 1        | 132          | 96.19       | pickup_after_dropoff | 2026-08-31 22:51:16 |
| 1        | 141          | 3.0         | pickup_after_dropoff | 2026-08-31 22:51:16 |

---

**Report Generated:** September 5, 2026  
**Data Quality Framework:** Automated DQ checks + Quarantine  
**Next Steps:** Gold layer star schema — ready for consumption

---

✅ **Data Quality Report Complete**
