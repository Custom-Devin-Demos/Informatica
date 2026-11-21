-- chk_ehrp2biis_wip_status_p (INFERRED — procedure source is not in the repo).
-- Inferred from the afterload: it runs right after PROCESS_TABLE.P_STARTDT is set from the
-- "a.gvt_wip_status <> b.gvt_wip_status AND a.changed_wip_status IS NULL" query, and the following
-- "Update cancelled actions" block re-loads events WHERE biis_wip_status_changed_dt = trunc(sysdate).
-- So: flag tracked actions whose eHRP WIP status changed (effdt >= P_STARTDT) with today's date.

-- @label chk_ehrp2biis_wip_status_p: flag tracked actions whose eHRP WIP status changed
MERGE INTO ${nknight}.ehrp_recs_tracking_tbl t
USING (SELECT DISTINCT a.emplid, a.empl_rcd, a.effdt, a.effseq, b.gvt_wip_status AS new_wip_status
         FROM ${nknight}.ehrp_recs_tracking_tbl a
         JOIN ${ehrp}.ps_gvt_job b
           ON a.emplid = b.emplid
          AND a.empl_rcd = b.empl_rcd
          AND a.effdt = b.effdt
          AND a.effseq = b.effseq
        WHERE a.gvt_wip_status <> b.gvt_wip_status
          AND a.changed_wip_status IS NULL
          AND a.effdt >= (SELECT min(p_startdt) FROM ${biis}.process_table)) s
   ON t.emplid = s.emplid AND t.empl_rcd = s.empl_rcd AND t.effdt = s.effdt AND t.effseq = s.effseq
  AND t.changed_wip_status IS NULL
WHEN MATCHED THEN UPDATE SET t.changed_wip_status = s.new_wip_status,
                             t.biis_wip_status_changed_dt = ${load_date};
