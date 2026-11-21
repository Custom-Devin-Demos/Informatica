-- update_sequence_number_tbl_p (INFERRED — procedure source is not in the repo).
-- exp_MAIN2BIIS numbers events EHRP_SEQ_NUMBER + 1, + 2 ... per GET_DATE_PART(EFFDT, 'YYYY'), so before the next
-- run SEQUENCE_NUM_TBL must hold the highest event id issued per year. 9xxxxxxxxx ids (the "900s" handled by
-- the HISTDBA formatting procedures) are excluded, as in afterload step 04.

-- @label update_sequence_number_tbl_p: advance EHRP_SEQ_NUMBER to today's max event id per year
MERGE INTO ${nknight}.sequence_num_tbl t
USING (SELECT year(p.event_eff_dte) AS ehrp_year, max(p.event_id) AS max_event_id
         FROM ${nknight}.nwk_action_primary_tbl p
        WHERE p.load_date = ${load_date}
          AND p.event_id < 9000000000
        GROUP BY year(p.event_eff_dte)) s
   ON t.ehrp_year = s.ehrp_year
WHEN MATCHED AND s.max_event_id > t.ehrp_seq_number THEN UPDATE SET t.ehrp_seq_number = s.max_event_id
WHEN NOT MATCHED THEN INSERT (ehrp_year, ehrp_seq_number) VALUES (s.ehrp_year, s.max_event_id);
