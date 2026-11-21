-- Prompt  Display the contents of the sequence number table (run before and after update_sequence_number_tbl_p).

-- @label afterload: NKNIGHT.SEQUENCE_NUM_TBL
SELECT * FROM ${nknight}.sequence_num_tbl ORDER BY 1;
