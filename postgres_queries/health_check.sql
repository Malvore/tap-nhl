-- Basic sanity checks for target-postgres with tap-nhl
-- Run with: psql "$TARGET_POSTGRES_DATABASE" -f postgres_queries/health_check.sql

-- Row counts and season coverage for each stream
SELECT
  'skaters' AS stream,
  COUNT(*) AS row_count,
  MIN("featuredStats__season") AS min_featured_season,
  MAX("featuredStats__season") AS max_featured_season
FROM tap_nhl.skaters
UNION ALL
SELECT
  'goalies' AS stream,
  COUNT(*) AS row_count,
  MIN("featuredStats__season") AS min_featured_season,
  MAX("featuredStats__season") AS max_featured_season
FROM tap_nhl.goalies;

-- Null checks on primary keys
SELECT
  'skaters' AS stream,
  COUNT(*) AS null_player_ids
FROM tap_nhl.skaters
WHERE "playerId" IS NULL
UNION ALL
SELECT
  'goalies' AS stream,
  COUNT(*) AS null_player_ids
FROM tap_nhl.goalies
WHERE "playerId" IS NULL;

-- Oldest and newest sample records per stream (by featured season)
(SELECT 'skaters' AS stream, "playerId", "featuredStats__season" AS season
 FROM tap_nhl.skaters
 ORDER BY "featuredStats__season" ASC
 LIMIT 3)
UNION ALL
(SELECT 'skaters' AS stream, "playerId", "featuredStats__season" AS season
 FROM tap_nhl.skaters
 ORDER BY "featuredStats__season" DESC
 LIMIT 3)
UNION ALL
(SELECT 'goalies' AS stream, "playerId", "featuredStats__season" AS season
 FROM tap_nhl.goalies
 ORDER BY "featuredStats__season" ASC
 LIMIT 3)
UNION ALL
(SELECT 'goalies' AS stream, "playerId", "featuredStats__season" AS season
 FROM tap_nhl.goalies
 ORDER BY "featuredStats__season" DESC
 LIMIT 3);
