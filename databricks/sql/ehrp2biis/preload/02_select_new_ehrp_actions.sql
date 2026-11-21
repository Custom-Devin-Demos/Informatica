-- INFERRED: new eHRP personnel actions = PS_GVT_JOB rows in a "new action" WIP status (default 'PRO',
-- job parameter NEW_ACTION_WIP_STATUSES) that BIIS has not tracked yet (EHRP_RECS_TRACKING_TBL is the
-- per-action audit written by s_m_EHRP2BIIS_UPDATE). Columns = the NWK_NEW_EHRP_ACTIONS_TBL source definition.

-- @label preload: select new eHRP actions not yet loaded to BIIS
INSERT INTO ${nknight}.nwk_new_ehrp_actions_tbl (emplid, empl_rcd, effdt, effseq)
SELECT DISTINCT j.emplid, j.empl_rcd, j.effdt, j.effseq
  FROM ${ehrp}.ps_gvt_job j
 WHERE j.gvt_wip_status IN (${new_action_wip_statuses})
   AND NOT EXISTS (SELECT 1
                     FROM ${nknight}.ehrp_recs_tracking_tbl t
                    WHERE t.emplid = j.emplid
                      AND t.empl_rcd = j.empl_rcd
                      AND t.effdt = j.effdt
                      AND t.effseq = j.effseq);
