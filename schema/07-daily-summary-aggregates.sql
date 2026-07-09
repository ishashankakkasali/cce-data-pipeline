-- CCE Analytics ClickHouse Schema — Daily KPI Aggregates (Refreshable, APPEND mode)
-- Run: clickhouse-client --database cce_analytics < schema/07-daily-summary-aggregates.sql
--
-- WHY THIS EXISTS
-- Compliance, facility, and deviation KPIs require either current mutable state
-- (protocol_instances, step_instances) or cross-source aggregation — neither can be done
-- with standard incremental MVs without double-counting CDC UPDATE rows. Refreshable MVs
-- (ClickHouse 24.3+) solve this by running a full SELECT every 30 minutes.
--
-- DOMAIN SPLIT — one MV per domain, reused across pages:
--
--   mv_daily_compliance_kpis           → Compliance page header + step metrics + deviation breakdown
--                                        Dashboard compliance cards (trackedPatients, rate)
--
--   mv_daily_facility_kpis             → Facilities page ranking table
--                                        Dashboard top/bottom facility lists
--
--   mv_daily_facility_activity_summary → Dashboard facility activity cards
--                                        (active_facilities / inactive_facilities / active_facility_rate_pct)
--                                        Facilities page header cards
--                                        (single row per day; DEPENDS ON mv_daily_facility_kpis_mv)
--                                        Requires: facility (schema/08)
--
--   mv_daily_adoption_kpis             → e-Buzima adoption indicator (per facility, per day)
--                                        actual patients vs expected patients per day × 100
--                                        (reporting_gap = expected − actual)
--                                        Requires: facility (schema/08)
--
--   mv_daily_deviation_kpis            → Deviations page header cards
--                                        (total / overdue / missed / order violation per protocol)
--
--   mv_daily_event_kpis                → Events page header cards
--                                        (total / matched / zero-match / duplicate / pipeline loss)
--
-- WHAT IS NOT COVERED (requires live queries to base tables):
--   - getDeviations()            paginated row-level list, runtime filters
--   - getDeviationsByAction()    step-keyed aggregation (no step-keyed MV exists yet)
--   - recentActivity windows     last24h/7d/30d rolling counts — incompatible with 30-min refresh
--   - getAtRiskHotspots()        three-way patient segmentation (on_track/at_risk/non_compliant)
--   - Patient timeline/detail    individual patient data — inherently row-level
--   - rank ordinal               runtime window function over live results
--   - patientsFromHIE            requires live filter on inbound_event_logs by source
--   - facility_name              resolved via dictionary/lookup at query time, not stored
--   - Trend charts               already covered by schema/03 MVs (mv_deviation_trends,
--                                mv_event_volume_hourly, mv_ingestion_quality, etc.)
--
-- APPEND MODE — how daily history works:
--
--   Each 30-minute refresh APPENDs a new set of rows into the backing table.
--   The backing tables use ReplacingMergeTree(refreshed_at) with ORDER BY starting
--   on (snapshot_date, <dimension_key>). This means:
--
--     - Within the same calendar day, multiple refreshes produce multiple rows for
--       (snapshot_date, dimension_key). ClickHouse background merges deduplicate them,
--       keeping only the row with the latest refreshed_at.
--
--     - Across different days, rows are retained permanently — one final row per
--       (day, dimension_key) survives after dedup.
--
--   Example for mv_daily_compliance_kpis (per protocol_definition_id):
--
--     snapshot_date | protocol_definition_id | tracked_patients | refreshed_at
--     2026-06-20    | proto-A                | 78               | 2026-06-20 23:30:00   ← last refresh day1
--     2026-06-21    | proto-A                | 82               | 2026-06-21 23:30:00   ← last refresh day2
--     2026-06-22    | proto-A                | 89               | 2026-06-22 10:00:00   ← current, updates every 30min
--
--   Querying:
--     Latest snapshot:    SELECT ... FROM mv_daily_compliance_kpis FINAL WHERE snapshot_date = today()
--     Specific day:       SELECT ... FROM mv_daily_compliance_kpis FINAL WHERE snapshot_date = '2026-06-20'
--     Trend across days:  SELECT snapshot_date, sum(tracked_patients) ... GROUP BY snapshot_date ORDER BY snapshot_date
--
--   FINAL is required to see deduplicated rows (suppresses multiple 30-min inserts
--   within the same day before ClickHouse background merge runs).
--
-- INITIAL REFRESH (run once immediately after applying this script):
--   SYSTEM REFRESH VIEW mv_daily_compliance_kpis_mv;
--   SYSTEM REFRESH VIEW mv_daily_facility_kpis_mv;
--   SYSTEM REFRESH VIEW mv_daily_facility_activity_summary_mv;   -- runs after facility_kpis (DEPENDS ON)
--   SYSTEM REFRESH VIEW mv_daily_deviation_kpis_mv;
--   SYSTEM REFRESH VIEW mv_daily_event_kpis_mv;
--   SYSTEM REFRESH VIEW mv_daily_adoption_kpis_mv;
--
-- Prerequisites: schema/06 rollup tables must be populated.
--                schema/08 (facility) must exist and contain at least one row.

USE cce_analytics;

-- ============================================================
-- 1. Compliance KPIs  (per snapshot_date × protocol_definition_id)
-- ============================================================
-- Covers: Compliance page (all header fields), Dashboard compliance cards.
--
-- Mirrors the backend aggregations:
--   ComplianceSummaryService.getAllProtocolsComplianceSummary()
--     → StepInstanceRepository.aggregateStepMetrics()      (step_* columns)
--     → DeviationRepository.aggregateDeviationMetrics()    (deviation_* columns)
--     → ProtocolInstanceRepository.countByStatus()         (status_* columns)
--
-- NOTE: tracked_patients counts ALL non-deleted enrollments (all statuses), matching
-- the backend's LEFT JOIN pattern (not filtered to ACTIVE-only).

CREATE TABLE IF NOT EXISTS mv_daily_compliance_kpis
(
    snapshot_date               Date,             -- calendar day this snapshot represents
    refreshed_at                DateTime64(3),    -- version: latest 30-min refresh wins per day

    protocol_definition_id      UUID,

    -- Enrollment status breakdown (maps to ComplianceSummary.statusBreakdown)
    total_enrollments           UInt32,
    status_active               UInt32,
    status_completed            UInt32,
    status_withdrawn            UInt32,
    status_expired              UInt32,

    -- Patient compliance (maps to ComplianceSummary patient fields)
    tracked_patients            UInt32,          -- = total_enrollments (non-deleted)
    compliant_count             UInt32,          -- enrollments with zero deviations
    non_compliant_count         UInt32,          -- enrollments with one or more deviations
    compliance_rate_pct         Float32,

    -- Deviation breakdown (maps to ComplianceSummary.deviationBreakdown)
    total_deviations            UInt32,
    overdue_deviations          UInt32,
    missed_deviations           UInt32,
    order_violation_deviations  UInt32,

    -- Step metrics (maps to ComplianceSummary.stepMetrics)
    step_total                  UInt32,
    step_completed              UInt32,          -- state IN (COMPLETED, SKIPPED)
    step_overdue                UInt32,
    step_missed                 UInt32,
    step_due                    UInt32,
    step_pending                UInt32,
    step_on_time                UInt32,          -- completion_status = ON_TIME
    step_early                  UInt32,          -- completion_status = EARLY
    step_late                   UInt32           -- completion_status = LATE

) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (snapshot_date, protocol_definition_id);
-- ReplacingMergeTree(refreshed_at): within the same (snapshot_date, protocol_definition_id)
-- the row with the latest refreshed_at survives — one authoritative row per day per protocol.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_compliance_kpis_mv
REFRESH EVERY 30 MINUTE APPEND
TO mv_daily_compliance_kpis
AS
WITH
-- 1. Resolve current state for every enrollment from argMaxState rollup.
--    protocol_definition_id and id are ORDER BY key columns (plain, not AggregateFunction).
all_instances AS (
    SELECT
        protocol_definition_id,
        id                               AS protocol_instance_id,
        argMaxMerge(status)              AS enrollment_status,
        toUInt8(argMaxMerge(is_deleted)) AS is_deleted
    FROM rollup_protocol_instance_current
    GROUP BY protocol_definition_id, id
),
-- 2. Non-deleted enrollments (all statuses) — used for status breakdown and compliance counts.
live_instances AS (
    SELECT protocol_definition_id, protocol_instance_id, enrollment_status
    FROM all_instances
    WHERE is_deleted = 0
),
-- 3. Enrollment status breakdown per protocol.
enrollment_agg AS (
    SELECT
        protocol_definition_id,
        toUInt32(count())                                    AS total_enrollments,
        toUInt32(countIf(enrollment_status = 'ACTIVE'))      AS status_active,
        toUInt32(countIf(enrollment_status = 'COMPLETED'))   AS status_completed,
        toUInt32(countIf(enrollment_status = 'WITHDRAWN'))   AS status_withdrawn,
        toUInt32(countIf(enrollment_status = 'EXPIRED'))     AS status_expired
    FROM live_instances
    GROUP BY protocol_definition_id
),
-- 4. Deviation counts per enrollment (all statuses, deviations are append-only).
instance_deviations AS (
    SELECT
        li.protocol_definition_id,
        li.protocol_instance_id,
        countIf(d.id != toUUID('00000000-0000-0000-0000-000000000000'))       AS deviation_count,
        countIf(d.deviation_type = 'OVERDUE')               AS overdue_count,
        countIf(d.deviation_type = 'MISSED')                AS missed_count,
        countIf(d.deviation_type = 'ORDER_VIOLATION')       AS order_violation_count
    FROM live_instances li
    LEFT JOIN deviations d ON li.protocol_instance_id = d.protocol_instance_id
    GROUP BY li.protocol_definition_id, li.protocol_instance_id
),
-- 5. Patient compliance and deviation summary per protocol.
patient_agg AS (
    SELECT
        protocol_definition_id,
        toUInt32(count())                                   AS tracked_patients,
        toUInt32(countIf(deviation_count = 0))              AS compliant_count,
        toUInt32(countIf(deviation_count > 0))              AS non_compliant_count,
        toUInt32(sum(deviation_count))                      AS total_deviations,
        toUInt32(sum(overdue_count))                        AS overdue_deviations,
        toUInt32(sum(missed_count))                         AS missed_deviations,
        toUInt32(sum(order_violation_count))                AS order_violation_deviations
    FROM instance_deviations
    GROUP BY protocol_definition_id
),
-- 6. Step metrics from rollup_step_current.
--    Inner query: resolve one current row per step (protocol_instance_id, id).
--    Outer: aggregate counts per protocol_definition_id via the live_instances join.
step_agg AS (
    SELECT
        li.protocol_definition_id,
        toUInt32(count())                                              AS step_total,
        toUInt32(countIf(rs.rs_state IN ('COMPLETED', 'SKIPPED')))    AS step_completed,
        toUInt32(countIf(rs.rs_state = 'OVERDUE'))                    AS step_overdue,
        toUInt32(countIf(rs.rs_state = 'MISSED'))                     AS step_missed,
        toUInt32(countIf(rs.rs_state = 'DUE'))                        AS step_due,
        toUInt32(countIf(rs.rs_state = 'PENDING'))                    AS step_pending,
        toUInt32(countIf(rs.rs_completion_status = 'ON_TIME'))         AS step_on_time,
        toUInt32(countIf(rs.rs_completion_status = 'EARLY'))           AS step_early,
        toUInt32(countIf(rs.rs_completion_status = 'LATE'))            AS step_late
    FROM (
        -- One resolved row per (protocol_instance_id, step_id)
        SELECT
            protocol_instance_id,
            id,
            argMaxMerge(state)               AS rs_state,
            argMaxMerge(completion_status)   AS rs_completion_status,
            toUInt8(argMaxMerge(is_deleted)) AS rs_is_deleted
        FROM rollup_step_current
        GROUP BY protocol_instance_id, id
    ) rs
    INNER JOIN live_instances li ON rs.protocol_instance_id = li.protocol_instance_id
    WHERE rs.rs_is_deleted = 0
    GROUP BY li.protocol_definition_id
)
SELECT
    toDate(now())                                                                        AS snapshot_date,
    now64(3)                                                                             AS refreshed_at,
    ea.protocol_definition_id                                                            AS protocol_definition_id,
    -- Enrollment breakdown
    ea.total_enrollments,
    ea.status_active,
    ea.status_completed,
    ea.status_withdrawn,
    ea.status_expired,
    -- Patient compliance
    pa.tracked_patients,
    pa.compliant_count,
    pa.non_compliant_count,
    coalesce(toFloat32(round(pa.compliant_count / nullIf(pa.tracked_patients, 0) * 100, 1)), 0.0) AS compliance_rate_pct,
    -- Deviation breakdown
    pa.total_deviations,
    pa.overdue_deviations,
    pa.missed_deviations,
    pa.order_violation_deviations,
    -- Step metrics
    coalesce(sa.step_total,     0)   AS step_total,
    coalesce(sa.step_completed, 0)   AS step_completed,
    coalesce(sa.step_overdue,   0)   AS step_overdue,
    coalesce(sa.step_missed,    0)   AS step_missed,
    coalesce(sa.step_due,       0)   AS step_due,
    coalesce(sa.step_pending,   0)   AS step_pending,
    coalesce(sa.step_on_time,   0)   AS step_on_time,
    coalesce(sa.step_early,     0)   AS step_early,
    coalesce(sa.step_late,      0)   AS step_late
FROM enrollment_agg ea
LEFT JOIN patient_agg pa ON ea.protocol_definition_id = pa.protocol_definition_id
LEFT JOIN step_agg    sa ON ea.protocol_definition_id = sa.protocol_definition_id;


-- ============================================================
-- 2. Facility KPIs  (per snapshot_date × facility_id)
-- ============================================================
-- Covers: Facilities page ranking table, Dashboard top/bottom facility lists.
--
-- Mirrors FacilityRankingService.getRankings():
--   compliance_rate_pct  ← StepInstanceRepository.aggregateStepMetricsByFacility()
--   total_deviations     ← DeviationRepository.countDeviationsByFacility()
--   event_count          ← pre-aggregated from mv_event_volume_hourly (today)
--
-- NOT stored (resolved at query time):
--   facility_name   → dictionary lookup / compliance_event_logs resource_body JSON
--   patientsFromHIE → inbound_event_logs filtered by source = 'ebuzima'
--   rank            → window function over live result set

CREATE TABLE IF NOT EXISTS mv_daily_facility_kpis
(
    snapshot_date          Date,
    refreshed_at           DateTime64(3),
    facility_id            String,
    tracked_patients       UInt32,               -- ACTIVE enrollments at this facility
    compliant_patients     UInt32,
    non_compliant_patients UInt32,
    compliance_rate_pct    Float32,
    total_deviations       UInt32,
    event_count            UInt64               -- accepted events today
) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (snapshot_date, facility_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_facility_kpis_mv
REFRESH EVERY 30 MINUTE APPEND
TO mv_daily_facility_kpis
AS
WITH
resolved_instances AS (
    SELECT
        id                               AS protocol_instance_id,
        argMaxMerge(patient_id)          AS patient_id,
        argMaxMerge(status)              AS enrollment_status,
        toUInt8(argMaxMerge(is_deleted)) AS is_deleted
    FROM rollup_protocol_instance_current
    GROUP BY id
),
instance_with_facility AS (
    -- Resolve each patient's most recently seen facility.
    -- pfl FINAL: ReplacingMergeTree — one row per patient after dedup.
    SELECT
        ri.protocol_instance_id,
        ri.patient_id,
        coalesce(pfl.facility_id, '') AS facility_id
    FROM resolved_instances ri
    LEFT JOIN mv_patient_facility_latest AS pfl FINAL
           ON ri.patient_id = pfl.patient_id
    WHERE ri.is_deleted = 0
      AND ri.enrollment_status = 'ACTIVE'
),
instance_deviations AS (
    SELECT
        iwf.protocol_instance_id,
        iwf.facility_id,
        countIf(d.id != toUUID('00000000-0000-0000-0000-000000000000')) AS deviation_count
    FROM instance_with_facility iwf
    LEFT JOIN deviations d ON iwf.protocol_instance_id = d.protocol_instance_id
    GROUP BY iwf.protocol_instance_id, iwf.facility_id
),
compliance_by_facility AS (
    SELECT
        facility_id,
        toUInt32(count())                   AS tracked_patients,
        toUInt32(countIf(deviation_count = 0)) AS compliant_patients,
        toUInt32(countIf(deviation_count > 0)) AS non_compliant_patients,
        coalesce(toFloat32(round(
            countIf(deviation_count = 0) / nullIf(count(), 0) * 100, 1
        )), 0.0)                            AS compliance_rate_pct,
        toUInt32(sum(deviation_count))      AS total_deviations
    FROM instance_deviations
    GROUP BY facility_id
),
events_today AS (
    -- Sum today's accepted events per facility from the pre-aggregated hourly MV.
    SELECT facility_id, sum(event_count) AS event_count
    FROM mv_event_volume_hourly
    WHERE toDate(hour) = toDate(now())
      AND facility_id  != ''
    GROUP BY facility_id
)
SELECT
    toDate(now())                       AS snapshot_date,
    now64(3)                            AS refreshed_at,
    cbf.facility_id,
    cbf.tracked_patients,
    cbf.compliant_patients,
    cbf.non_compliant_patients,
    cbf.compliance_rate_pct,
    cbf.total_deviations,
    coalesce(et.event_count, 0)         AS event_count
FROM compliance_by_facility cbf
LEFT JOIN events_today et ON cbf.facility_id = et.facility_id
WHERE cbf.facility_id != '';           -- exclude unattributed patients


-- ============================================================
-- 3. Facility Activity Summary  (per snapshot_date — single row per day)
-- ============================================================
-- Covers: Dashboard facility activity cards, Facilities page header cards.
--
-- Replaces the old compliance-tier bucket cards (>90 / 75-90 / <75) with:
--   total_in_scope           → row count of facility (all rows = all in-scope facilities)
--   active_facilities        → in-scope facilities that transmitted ≥1 HIE event today
--                              (event_count > 0 in mv_daily_facility_kpis for this snapshot_date)
--   inactive_facilities      → facilities in reference list but no events today
--   active_facility_rate_pct → active / total_in_scope × 100
--
-- "Active" is defined per the programme specification:
--   a facility is active for a period if it has transmitted ANY data through the HIE
--   during that period. Here the period is always "today" (ClickHouse server date).
--   For reporting-period-aware queries (e.g. date range from UI) the backend service
--   should query mv_event_volume_hourly directly with the desired date filter.
--
-- Requires: facility (schema/08) populated.
-- DEPENDS ON mv_daily_facility_kpis_mv: runs after facility KPIs are refreshed.

CREATE TABLE IF NOT EXISTS mv_daily_facility_activity_summary
(
    snapshot_date              Date,
    refreshed_at               DateTime64(3),
    total_in_scope             UInt32,        -- total facilities in the agreed facility list
    active_facilities          UInt32,        -- transmitted ≥1 event today
    inactive_facilities        UInt32,        -- in scope but no events today
    active_facility_rate_pct   Float32        -- active / total_in_scope × 100
) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (snapshot_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_facility_activity_summary_mv
REFRESH EVERY 30 MINUTE DEPENDS ON mv_daily_facility_kpis_mv APPEND
TO mv_daily_facility_activity_summary
AS
SELECT
    toDate(now())                                                                     AS snapshot_date,
    now64(3)                                                                          AS refreshed_at,
    toUInt32(count())                                                                 AS total_in_scope,
    toUInt32(countIf(fk.event_count > 0))                                             AS active_facilities,
    toUInt32(countIf(isNull(fk.event_count) OR fk.event_count = 0))                   AS inactive_facilities,
    coalesce(toFloat32(round(
        countIf(fk.event_count > 0) / nullIf(count(), 0) * 100, 1
    )), 0.0)                                                                          AS active_facility_rate_pct
FROM (
    SELECT facility_id
    FROM cce_analytics.facility
    WHERE _is_deleted = 0
) AS fr
LEFT JOIN mv_daily_facility_kpis fk
       ON fr.facility_id = fk.facility_id
      AND fk.snapshot_date = toDate(now());   -- join only today's facility kpis rows


-- ============================================================
-- 4. Deviation KPIs  (per snapshot_date × protocol_definition_id)
-- ============================================================
-- Covers: Deviations page header cards (total / overdue / missed / order violation).
-- For the deviation trend chart use mv_deviation_trends (schema/03).
-- For the paginated deviation list and deviations-by-action, live queries are required.
--
-- NOTE: protocol_definition_id is a plain ORDER BY column in rollup_protocol_instance_current
-- (not an AggregateFunction) — it is selected directly in the GROUP BY, not via argMaxMerge.

CREATE TABLE IF NOT EXISTS mv_daily_deviation_kpis
(
    snapshot_date          Date,
    refreshed_at           DateTime64(3),
    protocol_definition_id UUID,
    total_deviations       UInt32,
    overdue_count          UInt32,
    missed_count           UInt32,
    order_violation_count  UInt32
) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (snapshot_date, protocol_definition_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_deviation_kpis_mv
REFRESH EVERY 30 MINUTE APPEND
TO mv_daily_deviation_kpis
AS
WITH
resolved_instances AS (
    -- protocol_definition_id is a plain column (ORDER BY key), not aggregated.
    SELECT
        protocol_definition_id,
        id                               AS protocol_instance_id,
        toUInt8(argMaxMerge(is_deleted)) AS is_deleted
    FROM rollup_protocol_instance_current
    GROUP BY protocol_definition_id, id
)
SELECT
    toDate(now())                                                        AS snapshot_date,
    now64(3)                                                             AS refreshed_at,
    ri.protocol_definition_id,
    toUInt32(count(d.id))                                                AS total_deviations,
    toUInt32(countIf(d.deviation_type = 'OVERDUE'))                      AS overdue_count,
    toUInt32(countIf(d.deviation_type = 'MISSED'))                       AS missed_count,
    toUInt32(countIf(d.deviation_type = 'ORDER_VIOLATION'))              AS order_violation_count
FROM resolved_instances ri
INNER JOIN deviations d ON ri.protocol_instance_id = d.protocol_instance_id
WHERE ri.is_deleted = 0
GROUP BY ri.protocol_definition_id;


-- ============================================================
-- 5. Event Processing KPIs  (per snapshot_date — single row per day)
-- ============================================================
-- Covers: Events page header cards (total events / matched rate / zero-match / duplicate / pipeline loss).
-- For per-resource-type, per-facility, per-practitioner, per-source breakdowns use
-- mv_event_volume_hourly, mv_facility_summary, mv_practitioner_summary (all schema/03).
--
-- total_events    → inbound_event_logs ACCEPTED count  (via mv_event_volume_hourly, all time)
-- matched_count   → compliance_event_logs processing_status = MATCHED
-- zero_match      → processing_status = ZERO_MATCH
-- duplicate       → processing_status = DUPLICATE
-- pipeline_loss   → accepted events that never reached compliance_event_logs

CREATE TABLE IF NOT EXISTS mv_daily_event_kpis
(
    snapshot_date         Date,
    refreshed_at          DateTime64(3),
    total_events          UInt64,
    matched_count         UInt64,
    zero_match_count      UInt64,
    duplicate_count       UInt64,
    matched_rate_pct      Float32,
    zero_match_rate_pct   Float32,
    pipeline_loss_count   Int64
) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (snapshot_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_event_kpis_mv
REFRESH EVERY 30 MINUTE APPEND
TO mv_daily_event_kpis
AS
WITH
inbound AS (
    -- Total accepted events from the pre-aggregated hourly MV (cheaper than scanning base table).
    SELECT sum(event_count) AS total_events FROM mv_event_volume_hourly
),
processed AS (
    SELECT
        countIf(processing_status = 'MATCHED')    AS matched_count,
        countIf(processing_status = 'ZERO_MATCH') AS zero_match_count,
        countIf(processing_status = 'DUPLICATE')  AS duplicate_count,
        count()                                   AS total_processed
    FROM compliance_event_logs
)
SELECT
    toDate(now())                                                                  AS snapshot_date,
    now64(3)                                                                       AS refreshed_at,
    (SELECT total_events     FROM inbound)                                         AS total_events,
    (SELECT matched_count    FROM processed)                                       AS matched_count,
    (SELECT zero_match_count FROM processed)                                       AS zero_match_count,
    (SELECT duplicate_count  FROM processed)                                       AS duplicate_count,
    coalesce(toFloat32(round(
        (SELECT matched_count   FROM processed) /
        nullIf((SELECT total_processed FROM processed), 0) * 100, 1
    )), 0.0)                                                                       AS matched_rate_pct,
    coalesce(toFloat32(round(
        (SELECT zero_match_count FROM processed) /
        nullIf((SELECT total_processed FROM processed), 0) * 100, 1
    )), 0.0)                                                                       AS zero_match_rate_pct,
    toInt64((SELECT total_events   FROM inbound)) -
    toInt64((SELECT total_processed FROM processed))                               AS pipeline_loss_count;


-- ============================================================
-- 6. e-Buzima Adoption KPIs  (per snapshot_date × facility_id)
-- ============================================================
-- Covers: e-Buzima Adoption indicator on the Dashboard / Facilities page.
--
-- Metrics (per facility per day):
--   actual_patients          → unique patients whose events reached the HIE on snapshot_date
--                              (uniqMerge from mv_facility_summary for that day)
--   expected_patients_per_day → static validated baseline from facility
--   adoption_rate_pct        → actual / expected × 100
--   reporting_gap            → expected − actual
--                              positive = under-reporting; negative = over-reporting
--
-- Requires: facility (schema/08) with expected_patients_per_day populated.
-- Does NOT depend on mv_daily_facility_kpis — reads mv_facility_summary directly.
--
-- For multi-day reporting periods (date range from UI) the backend service should:
--   SELECT snapshot_date, facility_id, actual_patients, expected_patients_per_day
--   FROM mv_daily_adoption_kpis FINAL
--   WHERE snapshot_date BETWEEN :from AND :to
--   and compute:
--     total_actual   = sum(actual_patients)
--     total_expected = expected_patients_per_day × count(distinct snapshot_date)
--     period_rate    = total_actual / total_expected × 100

CREATE TABLE IF NOT EXISTS mv_daily_adoption_kpis
(
    snapshot_date              Date,
    refreshed_at               DateTime64(3),
    facility_id                String,
    expected_patients_per_day  UInt32,        -- from facility (static baseline)
    actual_patients            UInt32,        -- unique patients with events on snapshot_date
    adoption_rate_pct          Float32,       -- actual / expected × 100
    reporting_gap              Int64          -- expected − actual (positive = under-reporting)
) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (snapshot_date, facility_id);
-- facility_name is intentionally NOT stored — resolved at query time from the facility table.
-- Storing it caused duplicate rows when the name changed between 30-min refreshes
-- (GROUP BY facility_id, facility_name produced two groups for the same facility_id).

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_adoption_kpis_mv
REFRESH EVERY 30 MINUTE APPEND
TO mv_daily_adoption_kpis
AS
-- uniqIf(iel.subject, iel.subject != '') NOT uniq(iel.subject): this is a LEFT JOIN, so a
-- facility with no accepted events today still yields one row whose iel.subject is the empty
-- string (ClickHouse fills unmatched right-side columns with type defaults). Plain uniq('')
-- counts that as 1 distinct patient, making a zero-activity facility report actual_patients=1.
-- Filtering out '' makes an inactive facility correctly yield 0 while still emitting its row.
SELECT
    toDate(now())                                                                          AS snapshot_date,
    now64(3)                                                                               AS refreshed_at,
    fr.facility_id,
    fr.expected_patients_per_day,
    toUInt32(uniqIf(iel.subject, iel.subject != ''))                                       AS actual_patients,
    coalesce(toFloat32(round(
        toUInt32(uniqIf(iel.subject, iel.subject != '')) / nullIf(toFloat64(fr.expected_patients_per_day), 0) * 100, 1
    )), 0.0)                                                                               AS adoption_rate_pct,
    toInt64(fr.expected_patients_per_day) - toInt64(toUInt32(uniqIf(iel.subject, iel.subject != '')))  AS reporting_gap
FROM (
    SELECT facility_id, expected_patients_per_day
    FROM cce_analytics.facility
    WHERE _is_deleted = 0
) AS fr
LEFT JOIN cce_analytics.inbound_event_logs AS iel
       ON iel.facility_id = fr.facility_id
      AND toDate(iel.received_at) = toDate(now())
      AND iel.status = 'ACCEPTED'
GROUP BY fr.facility_id, fr.expected_patients_per_day;


-- ============================================================
-- QUERY EXAMPLES (page-by-page)
-- ============================================================
--
-- DASHBOARD — compliance cards (today's snapshot, all protocols):
--   SELECT sum(tracked_patients), sum(compliant_count), sum(non_compliant_count),
--     round(sum(compliant_count)/nullIf(sum(tracked_patients),0)*100,1) AS compliance_rate_pct
--   FROM mv_daily_compliance_kpis FINAL
--   WHERE snapshot_date = today();
--
-- DASHBOARD — facility activity cards (today):
--   SELECT total_in_scope, active_facilities, inactive_facilities, active_facility_rate_pct
--   FROM mv_daily_facility_activity_summary FINAL
--   WHERE snapshot_date = today()
--   LIMIT 1;
--
-- DASHBOARD — top 3 / bottom 3 facilities by compliance (today):
--   SELECT facility_id, compliance_rate_pct, total_deviations, event_count
--   FROM mv_daily_facility_kpis FINAL
--   WHERE snapshot_date = today()
--   ORDER BY compliance_rate_pct DESC LIMIT 3;
--
-- DASHBOARD — e-Buzima adoption overview today (worst under-reporters first):
--   SELECT a.facility_id, f.facility_name, a.expected_patients_per_day, a.actual_patients,
--          a.adoption_rate_pct, a.reporting_gap
--   FROM mv_daily_adoption_kpis a FINAL
--   LEFT JOIN facility f FINAL ON f.facility_id = a.facility_id
--   WHERE a.snapshot_date = today()
--   ORDER BY a.reporting_gap DESC;
--
-- TREND — compliance rate over last 30 days (daily snapshots):
--   SELECT snapshot_date,
--          sum(tracked_patients)  AS tracked_patients,
--          sum(compliant_count)   AS compliant_count,
--          round(sum(compliant_count)/nullIf(sum(tracked_patients),0)*100,1) AS compliance_rate_pct
--   FROM mv_daily_compliance_kpis FINAL
--   WHERE snapshot_date >= today() - 30
--   GROUP BY snapshot_date
--   ORDER BY snapshot_date;
--
-- TREND — adoption rate over a reporting period (multi-day):
--   SELECT a.snapshot_date, a.facility_id, f.facility_name,
--          a.actual_patients, a.expected_patients_per_day, a.adoption_rate_pct
--   FROM mv_daily_adoption_kpis a FINAL
--   LEFT JOIN facility f FINAL ON f.facility_id = a.facility_id
--   WHERE a.snapshot_date BETWEEN '2026-01-01' AND '2026-06-22'
--   ORDER BY a.snapshot_date, a.facility_id;
--
-- COMPLIANCE PAGE — per-protocol summary (today):
--   SELECT protocol_definition_id,
--     total_enrollments, status_active, status_completed, status_withdrawn, status_expired,
--     tracked_patients, compliant_count, non_compliant_count, compliance_rate_pct,
--     step_total, step_completed, step_on_time, step_late, step_early,
--     step_overdue, step_missed, step_due, step_pending,
--     total_deviations, overdue_deviations, missed_deviations, order_violation_deviations
--   FROM mv_daily_compliance_kpis FINAL
--   WHERE snapshot_date = today()
--     AND protocol_definition_id = ?;
--
-- FACILITIES PAGE — ranking table (today):
--   SELECT facility_id, compliance_rate_pct, tracked_patients, total_deviations, event_count,
--     row_number() OVER (ORDER BY compliance_rate_pct DESC) AS rank
--   FROM mv_daily_facility_kpis FINAL
--   WHERE snapshot_date = today()
--   ORDER BY compliance_rate_pct DESC;
--
-- FACILITIES PAGE — adoption table (today):
--   SELECT a.facility_id, f.facility_name, a.expected_patients_per_day, a.actual_patients,
--          a.adoption_rate_pct, a.reporting_gap
--   FROM mv_daily_adoption_kpis a FINAL
--   LEFT JOIN facility f FINAL ON f.facility_id = a.facility_id
--   WHERE a.snapshot_date = today()
--   ORDER BY a.adoption_rate_pct DESC;
--
-- DEVIATIONS PAGE — header cards (today):
--   SELECT sum(total_deviations), sum(overdue_count), sum(missed_count), sum(order_violation_count)
--   FROM mv_daily_deviation_kpis FINAL
--   WHERE snapshot_date = today();
--   -- omit snapshot_date filter to get all-time totals
--
-- EVENTS PAGE — header cards (today's refresh):
--   SELECT total_events, matched_rate_pct, zero_match_rate_pct, pipeline_loss_count
--   FROM mv_daily_event_kpis FINAL
--   WHERE snapshot_date = today()
--   LIMIT 1;
