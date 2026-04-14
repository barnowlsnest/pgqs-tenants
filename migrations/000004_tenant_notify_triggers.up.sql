BEGIN;

ALTER TABLE pgqs.tenants ADD COLUMN status TEXT NOT NULL DEFAULT 'created';

UPDATE pgqs.tenants SET status = 'disabled' WHERE is_deleted = true;

ALTER TABLE pgqs.tenants DROP COLUMN is_deleted;

ALTER TABLE pgqs.tenants ADD CONSTRAINT tenants_status_check
    CHECK (status IN ('created', 'ready', 'disabled'));

CREATE OR REPLACE FUNCTION pgqs.notify_tenant_status_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('tenants', jsonb_build_object(
        'id',     NEW.id,
        'schema', NEW.schema_name,
        'event',  NEW.status
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgqs.notify_tenant_purged()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('tenants', jsonb_build_object(
        'id',     OLD.id,
        'schema', OLD.schema_name,
        'event',  'purged'
    )::text);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_tenant_insert
    AFTER INSERT ON pgqs.tenants
    FOR EACH ROW
    EXECUTE FUNCTION pgqs.notify_tenant_status_change();

CREATE TRIGGER trg_notify_tenant_status_change
    AFTER UPDATE ON pgqs.tenants
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION pgqs.notify_tenant_status_change();

CREATE TRIGGER trg_notify_tenant_purged
    AFTER DELETE ON pgqs.tenants
    FOR EACH ROW
    EXECUTE FUNCTION pgqs.notify_tenant_purged();

COMMIT;
