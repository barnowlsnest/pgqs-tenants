CREATE TABLE IF NOT EXISTS pgqs.tenants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'created',
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT tenants_name_unique UNIQUE (name),
    CONSTRAINT tenants_status_check CHECK (state IN ('created', 'ready', 'disabled'))
);
