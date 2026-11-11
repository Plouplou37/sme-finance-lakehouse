-- Blocking: every check that can be measured must match the generator's manifest.
-- Returns the checks that do not (0 rows = pass). A mismatch means the platform lost, added or
-- misclassified events: the run must stop and someone must look.

select *
from {{ ref('quality_reconciliation') }}
where status = 'mismatch'
