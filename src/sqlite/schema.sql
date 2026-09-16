PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS deployments (
    namespace_id TEXT PRIMARY KEY,
    body TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deployment_contracts (
    namespace_id TEXT NOT NULL,
    code_revision TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL,
    PRIMARY KEY (namespace_id, code_revision)
);

CREATE TABLE IF NOT EXISTS host_leases (
    host_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    route TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS placements (
    object_id TEXT PRIMARY KEY,
    owner_host_id TEXT NOT NULL,
    owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
    home_region TEXT NOT NULL,
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    state_object TEXT,
    last_request_id TEXT
);
