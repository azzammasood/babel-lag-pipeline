-- Babel Lag bronze: sanitized EventStreams micro-batches picked up from the landing volume by
-- Auto Loader (read_files on a stream). Owner: babel-lag. Domain: knowledge-spread.
-- No personal data: usernames, IPs and comments are dropped before upload (stream/sanitize.py).
CREATE OR REFRESH STREAMING TABLE bronze_live_events (
  CONSTRAINT has_event_id EXPECT (meta.id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_event_time EXPECT (meta.dt IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_stream EXPECT (meta.stream IN ('mediawiki.recentchange',
    'mediawiki.revision-create', 'mediawiki.page-create'))
)
COMMENT 'Sanitized live Wikimedia change events (EventStreams), one row per event; CC0 source.'
TBLPROPERTIES ('quality' = 'bronze', 'contains_pii' = 'false')
AS SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/babel_landing/live/',
  format => 'json',
  schemaHints => 'meta STRUCT<id STRING, dt STRING, stream STRING, domain STRING, uri STRING>'
);
