-- Update cancelled actions: events whose eHRP WIP status changed today are deleted from *_ALL and
-- re-inserted from the NWK tables. DELETE ... WHERE IN (subquery) -> MERGE ... WHEN MATCHED THEN DELETE.

-- @label afterload: delete changed events from action_secondary_all
MERGE INTO ${biis}.action_secondary_all d
USING (SELECT DISTINCT b.biis_event_id
         FROM ${nknight}.ehrp_recs_tracking_tbl b
        WHERE b.biis_wip_status_changed_dt = ${load_date}) b
   ON d.event_id = b.biis_event_id
WHEN MATCHED THEN DELETE;

-- @label afterload: delete changed events from action_remarks_all
MERGE INTO ${biis}.action_remarks_all d
USING (SELECT DISTINCT b.biis_event_id
         FROM ${nknight}.ehrp_recs_tracking_tbl b
        WHERE b.biis_wip_status_changed_dt = ${load_date}) b
   ON d.event_id = b.biis_event_id
WHEN MATCHED THEN DELETE;

-- @label afterload: delete changed events from action_primary_all
MERGE INTO ${biis}.action_primary_all d
USING (SELECT DISTINCT b.biis_event_id
         FROM ${nknight}.ehrp_recs_tracking_tbl b
        WHERE b.biis_wip_status_changed_dt = ${load_date}) b
   ON d.event_id = b.biis_event_id
WHEN MATCHED THEN DELETE;

-- @label afterload: re-insert changed events into action_primary_all
INSERT INTO ${biis}.action_primary_all
SELECT * FROM ${nknight}.nwk_action_primary_tbl d
 WHERE d.event_id IN (SELECT b.biis_event_id
                        FROM ${nknight}.ehrp_recs_tracking_tbl b
                       WHERE b.biis_wip_status_changed_dt = ${load_date});

-- @label afterload: re-insert changed events into action_secondary_all
INSERT INTO ${biis}.action_secondary_all
SELECT * FROM ${nknight}.nwk_action_secondary_tbl d
 WHERE d.event_id IN (SELECT b.biis_event_id
                        FROM ${nknight}.ehrp_recs_tracking_tbl b
                       WHERE b.biis_wip_status_changed_dt = ${load_date});

-- @label afterload: re-insert changed events into action_remarks_all
INSERT INTO ${biis}.action_remarks_all
SELECT * FROM ${nknight}.nwk_action_remarks_tbl d
 WHERE d.event_id IN (SELECT b.biis_event_id
                        FROM ${nknight}.ehrp_recs_tracking_tbl b
                       WHERE b.biis_wip_status_changed_dt = ${load_date});
