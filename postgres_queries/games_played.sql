-- This file contains a collection of queries that measures number of games played

-- Top 25 skaters by number of regular season games played.
-- Adjust LIMIT 25 to your desired value to view a different number of results.
SELECT "firstName", "lastName", "careerTotals__regularSeason__gamesPlayed"
FROM tap_nhl.skaters
WHERE "careerTotals__regularSeason__gamesPlayed" IS NOT NULL
ORDER BY "careerTotals__regularSeason__gamesPlayed" DESC
LIMIT 25;

-- Top 25 skaters by number of playoff games played.
-- Adjust LIMIT 25 to your desired value to view a different number of results.
SELECT "firstName", "lastName", "careerTotals__playoffs__gamesPlayed"
FROM tap_nhl.skaters
WHERE "careerTotals__playoffs__gamesPlayed" IS NOT NULL
ORDER BY "careerTotals__playoffs__gamesPlayed" DESC
LIMIT 25;

-- Top 25 goalies by number of regular season games played.
-- Adjust LIMIT 25 to your desired value to view a different number of results.
SELECT "firstName", "lastName", "careerTotals__regularSeason__gamesPlayed"
FROM tap_nhl.goalies
WHERE "careerTotals__regularSeason__gamesPlayed" IS NOT NULL
ORDER BY "careerTotals__regularSeason__gamesPlayed" DESC
LIMIT 25;

-- Top 25 goalies by number of playoff games played.
-- Adjust LIMIT 25 to your desired value to view a different number of results.
SELECT "firstName", "lastName", "careerTotals__playoffs__gamesPlayed"
FROM tap_nhl.goalies
WHERE "careerTotals__playoffs__gamesPlayed" IS NOT NULL
ORDER BY "careerTotals__playoffs__gamesPlayed" DESC
LIMIT 25;