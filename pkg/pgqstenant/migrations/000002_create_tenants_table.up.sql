CREATE TABLE IF NOT EXISTS pgqs.tenants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'created',
    metadata        JSONB NOT NULL DEFAULT '{}',
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT tenants_name_unique UNIQUE (name),
    CONSTRAINT tenants_state_check CHECK (state IN ('created', 'ready', 'disabled'))
);
