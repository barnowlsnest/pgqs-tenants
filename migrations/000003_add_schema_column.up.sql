ALTER TABLE pgqs.tenants
    ADD COLUMN schema_name TEXT GENERATED ALWAYS AS ('pgqs_tenant_' || id::text) STORED;

CREATE INDEX idx_tenants_schema_name ON pgqs.tenants (schema_name);
