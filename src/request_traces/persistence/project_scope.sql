CREATE TABLE scoped_traces (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    event TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    project_id TEXT NOT NULL,
    UNIQUE(project_id, event_id)
);
INSERT INTO scoped_traces
SELECT position, event_id, event, started_at_ms, actor_name, actor_id, outcome,
       COALESCE(json_extract(event, '$.projectId'), '') FROM traces;
-- Retain the replay watermark even if retention has removed its last row.
UPDATE sqlite_sequence SET seq = MAX(seq, COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'traces'), 0)) WHERE name = 'scoped_traces';
DROP TABLE traces;
ALTER TABLE scoped_traces RENAME TO traces;
CREATE INDEX traces_time ON traces(started_at_ms DESC, position DESC);
CREATE INDEX traces_actor ON traces(actor_name, actor_id, started_at_ms DESC, position DESC);
CREATE INDEX traces_outcome ON traces(outcome, started_at_ms DESC, position DESC);
CREATE INDEX traces_project ON traces(project_id, started_at_ms DESC, position DESC);
