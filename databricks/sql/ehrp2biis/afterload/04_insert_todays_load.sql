-- Update BIIS with new data from today's load: NKNIGHT.NWK_ACTION_*_TBL -> *_ALL.
-- INSERT ... SELECT * is positional, exactly as in Oracle: the *_ALL tables must keep the NWK column order.

-- @label afterload: insert into action_primary_all
INSERT INTO ${biis}.action_primary_all
SELECT * FROM ${nknight}.nwk_action_primary_tbl c
 WHERE c.load_date = ${load_date};

-- @label afterload: insert into action_secondary_all
INSERT INTO ${biis}.action_secondary_all
SELECT * FROM ${nknight}.nwk_action_secondary_tbl c
 WHERE c.event_id IN (SELECT a.event_id
                        FROM ${nknight}.nwk_action_primary_tbl a
                       WHERE a.load_date = ${load_date});

-- @label afterload: insert into action_remarks_all
INSERT INTO ${biis}.action_remarks_all
SELECT * FROM ${nknight}.nwk_action_remarks_tbl c
 WHERE c.event_id IN (SELECT a.event_id
                        FROM ${nknight}.nwk_action_primary_tbl a
                       WHERE a.load_date = ${load_date});
