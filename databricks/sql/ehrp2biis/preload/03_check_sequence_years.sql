-- lkp_OLD_SEQUENCE_NUMBER (EHRP_YEAR = GET_DATE_PART(EFFDT, 'YYYY')) returns NULL for a year missing from
-- SEQUENCE_NUM_TBL, which makes exp_MAIN2BIIS.v_EVENT_ID NULL. Fail before the load instead.

-- @label preload: every new action year has a SEQUENCE_NUM_TBL row
-- @assert_empty NKNIGHT.SEQUENCE_NUM_TBL has no EHRP_YEAR row for new actions (event ids would be NULL)
SELECT DISTINCT year(n.effdt) AS ehrp_year
  FROM ${nknight}.nwk_new_ehrp_actions_tbl n
 WHERE NOT EXISTS (SELECT 1 FROM ${nknight}.sequence_num_tbl s WHERE s.ehrp_year = year(n.effdt))
 ORDER BY 1;
