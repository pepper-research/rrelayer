-- Additive while legacy code is running. Activation is a separate operator step.
ALTER TABLE relayer.transaction ADD COLUMN IF NOT EXISTS broadcast_attempted BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE relayer.transaction_audit_log ADD COLUMN IF NOT EXISTS broadcast_attempted BOOLEAN NOT NULL DEFAULT TRUE;
CREATE TABLE IF NOT EXISTS relayer.sender_deployment (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    protocol INTEGER NOT NULL DEFAULT 1 CHECK (protocol = 1),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    active_release TEXT
);
INSERT INTO relayer.sender_deployment(singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS relayer.sender_owner (
    chain_id BIGINT NOT NULL,
    signer BYTEA NOT NULL,
    token TEXT NOT NULL,
    PRIMARY KEY (chain_id, signer)
);
-- These are the existing transaction's signed attempts, not a second queue.
CREATE TABLE IF NOT EXISTS relayer.transaction_attempt (
    transaction_id UUID NOT NULL REFERENCES relayer.transaction(id),
    hash BYTEA NOT NULL,
    signed_envelope BYTEA NOT NULL,
    gas JSONB NOT NULL,
    blob_gas JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (transaction_id, hash)
);
CREATE OR REPLACE FUNCTION relayer.fence_transaction() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE expected TEXT;
BEGIN
    IF NOT (SELECT enabled FROM relayer.sender_deployment WHERE singleton) THEN RETURN NEW; END IF;
    -- Lock the epoch row through commit: a takeover cannot pass an unfinished write.
    SELECT token INTO expected FROM relayer.sender_owner
        WHERE chain_id = NEW.chain_id AND signer = NEW."from" FOR SHARE;
    IF expected IS NULL OR expected IS DISTINCT FROM current_setting('rrelayer.sender_token', true) THEN
        RAISE EXCEPTION 'stale or unmanaged sender' USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'UPDATE' AND (
        NEW.nonce <> OLD.nonce OR NEW.chain_id <> OLD.chain_id OR NEW."from" <> OLD."from" OR
        (OLD.broadcast_attempted AND (
            NOT NEW.broadcast_attempted OR NEW."to" <> OLD."to" OR NEW.value <> OLD.value OR
            NEW.data IS DISTINCT FROM OLD.data OR NEW.authorization_list IS DISTINCT FROM OLD.authorization_list OR
            NEW.blobs IS DISTINCT FROM OLD.blobs))) THEN
        RAISE EXCEPTION 'cannot change an assigned nonce or attempted payload' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END $$;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'sender_fence') THEN
        CREATE TRIGGER sender_fence BEFORE INSERT OR UPDATE ON relayer.transaction
            FOR EACH ROW EXECUTE FUNCTION relayer.fence_transaction();
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS transaction_sender_nonce ON relayer.transaction(chain_id, "from", nonce DESC);
ALTER TABLE relayer.transaction ADD COLUMN IF NOT EXISTS competition_kind TEXT CHECK (competition_kind IN ('cancel','replace'));
