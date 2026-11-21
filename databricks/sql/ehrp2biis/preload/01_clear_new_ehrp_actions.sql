-- ehrp2biis_preload / step01 (INFERRED — step01.sql is not in the repo).
-- wf_EHRP2BIIS_UPDATE (SQ_PS_GVT_JOB) loads exactly the rows keyed in NWK_NEW_EHRP_ACTIONS_TBL, and the
-- afterload empties it "before next load". The preload therefore (re)builds that driver table.
-- Oracle TRUNCATE -> Delta DELETE (works on OSS Delta and Unity Catalog alike).

-- @label preload: clear nknight.nwk_new_ehrp_actions_tbl
DELETE FROM ${nknight}.nwk_new_ehrp_actions_tbl;
