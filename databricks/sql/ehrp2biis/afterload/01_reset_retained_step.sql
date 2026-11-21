-- ehrp2biis_afterload.sql (Step 04) UPDATE_NWK_actsec_RETAINED_STEP.
-- Oracle UPDATE ... WHERE x IN (subquery) -> Delta MERGE (subquery conditions are not allowed in Delta UPDATE).

-- @label afterload step 04: null retnd1_step_cd '0.0000000000000' for today's load
MERGE INTO ${nknight}.nwk_action_secondary_tbl a
USING (SELECT DISTINCT b.event_id
         FROM ${nknight}.nwk_action_primary_tbl b
        WHERE b.load_date = ${load_date}
          AND b.event_id < 9000000000) b
   ON a.event_id = b.event_id
  AND a.retnd1_step_cd = '0.0000000000000'
WHEN MATCHED THEN UPDATE SET a.retnd1_step_cd = NULL;
