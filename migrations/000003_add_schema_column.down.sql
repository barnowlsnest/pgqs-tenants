DROP INDEX IF EXISTS pgqs.idx_tenants_schema_name;

ALTER TABLE pgqs.tenants
    DROP CONSTRAINT IF EXISTS tenants_schema_name_unique;

ALTER TABLE pgqs.tenants
    DROP COLUMN IF EXISTS schema_name;
