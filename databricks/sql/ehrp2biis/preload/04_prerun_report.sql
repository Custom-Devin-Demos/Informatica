-- ehrp2biis_prerun_rpt.txt equivalent (was uuencoded and mailed; now spooled to the landing log folder).

-- @label preload: new actions to load
-- @metric new_actions
SELECT count(*) AS new_actions FROM ${nknight}.nwk_new_ehrp_actions_tbl;

-- @label preload: new actions by NOA code / WIP status
SELECT j.gvt_noa_code, j.gvt_wip_status, count(*) AS actions
  FROM ${nknight}.nwk_new_ehrp_actions_tbl n
  JOIN ${ehrp}.ps_gvt_job j
    ON j.emplid = n.emplid AND j.empl_rcd = n.empl_rcd AND j.effdt = n.effdt AND j.effseq = n.effseq
 GROUP BY j.gvt_noa_code, j.gvt_wip_status
 ORDER BY 1, 2;

-- @label preload: current sequence numbers
SELECT * FROM ${nknight}.sequence_num_tbl ORDER BY 1;
