--
-- 9.4-1--9.4-2 was added later as a patch to fix a bug in our PG upgrade functions
--
-- This script brings users who installed the patch released back to the 9.4-1
-- upgrade path. We do this via a semantical downgrade since there has already been
-- introduced new changes in the schema from 9.4-1 to 9.5-1. To make sure we include all
-- changes made during that version change we decide to use the existing upgrade path from
-- our later introduced 9.4-2 version.
--