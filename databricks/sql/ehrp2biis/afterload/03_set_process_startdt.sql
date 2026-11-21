-- PROCESS_TABLE.P_STARTDT = earliest EFFDT whose eHRP WIP status changed since BIIS tracked it,
-- or trunc(sysdate) + 10000 when there is none. Read by chk_ehrp2biis_wip_status_p.
-- "(... ORDER BY 4, 1) WHERE ROWNUM < 2" -> ORDER BY effdt, biis_event_id LIMIT 1.
-- Oracle scalar-subquery UPDATE -> MERGE ON TRUE (Delta UPDATE does not accept subqueries).

-- @label afterload: UPDATE PROCESS_TABLE SET P_STARTDT = NULL
UPDATE ${biis}.process_table SET p_startdt = NULL;

-- @label afterload: UPDATE PROCESS_TABLE SET P_STARTDT = first changed-WIP-status effdt
MERGE INTO ${biis}.process_table p
USING (SELECT a.effdt
         FROM ${nknight}.ehrp_recs_tracking_tbl a
         JOIN ${ehrp}.ps_gvt_job b
           ON a.emplid = b.emplid
          AND a.empl_rcd = b.empl_rcd
          AND a.effdt = b.effdt
          AND a.effseq = b.effseq
        WHERE a.gvt_wip_status <> b.gvt_wip_status
          AND a.changed_wip_status IS NULL
        ORDER BY a.effdt, a.biis_event_id
        LIMIT 1) s
   ON TRUE
WHEN MATCHED THEN UPDATE SET p.p_startdt = s.effdt;

-- @label afterload: default P_STARTDT = trunc(sysdate) + 10000
UPDATE ${biis}.process_table
   SET p_startdt = date_add(${load_date}, 10000)
 WHERE p_startdt IS NULL;
