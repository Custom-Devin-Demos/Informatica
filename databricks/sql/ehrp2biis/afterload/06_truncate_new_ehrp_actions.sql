-- Truncating NEW_EHRP_ACTIONS_TABLE before next load - To avoid a load on Sunday.
-- Oracle TRUNCATE TABLE -> Delta DELETE (TRUNCATE is Unity-Catalog-only on Delta; DELETE keeps time travel).

-- @label afterload: truncate nknight.nwk_new_ehrp_actions_tbl
DELETE FROM ${nknight}.nwk_new_ehrp_actions_tbl;
