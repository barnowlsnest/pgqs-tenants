BEGIN;

DROP TRIGGER IF EXISTS trg_notify_tenant_insert ON pgqs.tenants;
DROP TRIGGER IF EXISTS trg_notify_tenant_status_change ON pgqs.tenants;
DROP TRIGGER IF EXISTS trg_notify_tenant_purged ON pgqs.tenants;

DROP FUNCTION IF EXISTS pgqs.notify_tenant_status_change();
DROP FUNCTION IF EXISTS pgqs.notify_tenant_purged();

ALTER TABLE pgqs.tenants ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT false;

UPDATE pgqs.tenants SET is_deleted = true WHERE status = 'disabled';

ALTER TABLE pgqs.tenants DROP CONSTRAINT IF EXISTS tenants_status_check;
ALTER TABLE pgqs.tenants DROP COLUMN status;

COMMIT;
