-- HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P(NULL) (REPLACEMENT — procedure source and its output table are not in
-- the repo). Writes per-run row counts to histdba.ehrp2biis_runcounts; re-running a load date replaces its rows.

-- @label gather_ehrp2biis_runcounts: ensure histdba.ehrp2biis_runcounts
CREATE TABLE IF NOT EXISTS ${histdba}.ehrp2biis_runcounts (
  load_date DATE,
  table_name STRING,
  row_count BIGINT,
  gathered_at TIMESTAMP
) USING DELTA;

-- @label gather_ehrp2biis_runcounts: clear counts for this load date
DELETE FROM ${histdba}.ehrp2biis_runcounts WHERE load_date = ${load_date};

-- @label gather_ehrp2biis_runcounts: gather counts
INSERT INTO ${histdba}.ehrp2biis_runcounts
WITH today AS (
  SELECT DISTINCT event_id FROM ${nknight}.nwk_action_primary_tbl WHERE load_date = ${load_date}
)
SELECT ${load_date}, table_name, row_count, current_timestamp()
  FROM (
    SELECT 'NWK_NEW_EHRP_ACTIONS_TBL' AS table_name, count(*) AS row_count FROM ${nknight}.nwk_new_ehrp_actions_tbl
    UNION ALL
    SELECT 'EHRP_RECS_TRACKING_TBL', count(*) FROM ${nknight}.ehrp_recs_tracking_tbl WHERE load_date = ${load_date}
    UNION ALL
    SELECT 'NWK_ACTION_PRIMARY_TBL', count(*) FROM ${nknight}.nwk_action_primary_tbl WHERE load_date = ${load_date}
    UNION ALL
    SELECT 'NWK_ACTION_SECONDARY_TBL', count(*) FROM ${nknight}.nwk_action_secondary_tbl
     WHERE event_id IN (SELECT event_id FROM today)
    UNION ALL
    SELECT 'NWK_ACTION_REMARKS_TBL', count(*) FROM ${nknight}.nwk_action_remarks_tbl
     WHERE event_id IN (SELECT event_id FROM today)
    UNION ALL
    SELECT 'ACTION_PRIMARY_ALL', count(*) FROM ${biis}.action_primary_all WHERE event_id IN (SELECT event_id FROM today)
    UNION ALL
    SELECT 'ACTION_SECONDARY_ALL', count(*) FROM ${biis}.action_secondary_all WHERE event_id IN (SELECT event_id FROM today)
    UNION ALL
    SELECT 'ACTION_REMARKS_ALL', count(*) FROM ${biis}.action_remarks_all WHERE event_id IN (SELECT event_id FROM today)
  );

-- @label gather_ehrp2biis_runcounts: counts
SELECT table_name, row_count FROM ${histdba}.ehrp2biis_runcounts WHERE load_date = ${load_date} ORDER BY 1;
