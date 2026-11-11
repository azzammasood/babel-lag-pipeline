-- silver_live_events: bronze de-duplicated on the event id (earliest arrival wins) with a
-- deterministic late flag. Watermark of a batch = the latest event time seen in all batches that
-- of the same stream arrived before it (streams replay at different speeds); an event older than its batch's watermark minus 600 s is late.
-- Portable SQL (DuckDB and Spark); EPOCH()/FROM_EPOCH() are rendered per engine.
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at, source_file) AS rn
  FROM bronze_live_events
  WHERE event_id IS NOT NULL AND event_dt IS NOT NULL AND wiki IS NOT NULL
),
batches AS (
  SELECT source_file, stream, min(ingested_at) AS batch_at, min(event_dt) AS batch_min_dt,
         max(event_dt) AS batch_max_dt
  FROM bronze_live_events
  GROUP BY source_file, stream
),
wm AS (
  SELECT source_file, stream,
         max(batch_max_dt) OVER (PARTITION BY stream ORDER BY batch_at, batch_min_dt, source_file
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS watermark_dt
  FROM batches
)
SELECT r.event_id, r.event_dt, r.stream, r.wiki, r.event_type, r.namespace, r.page_id,
       r.page_title, r.rev_id, r.is_bot, r.is_redirect, r.length_new, r.editor_hash,
       r.source_file, r.ingested_at,
       CASE WHEN w.watermark_dt IS NOT NULL
             AND EPOCH(r.event_dt) < EPOCH(w.watermark_dt) - 600 THEN true ELSE false END AS is_late
FROM ranked r
JOIN wm w ON r.source_file = w.source_file AND r.stream = w.stream
WHERE r.rn = 1
