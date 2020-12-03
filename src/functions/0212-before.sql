
CREATE OR REPLACE FUNCTION _rrule.before(
  "rrule" _rrule.RRULE,
  "dtstart" TIMESTAMP,
  "duration" INTERVAL,
  "when" TIMESTAMP
)
RETURNS SETOF TSRANGE AS $$
  SELECT *
  FROM _rrule.occurrences("rrule", "dtstart", "duration", tsrange(NULL, "when", '[]'));
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.before("rrule" TEXT, "dtstart" TIMESTAMP, "duration" INTERVAL, "when" TIMESTAMP)
RETURNS SETOF TSRANGE AS $$
  SELECT _rrule.before(_rrule.rrule("rrule"), "dtstart", "duration", "when");
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.before("rruleset" _rrule.RRULESET, "when" TIMESTAMP)
RETURNS SETOF TSRANGE AS $$
  SELECT *
  FROM _rrule.occurrences("rruleset", tsrange(NULL, "when", '[]'));
$$ LANGUAGE SQL STRICT IMMUTABLE;

-- TODO: test
CREATE OR REPLACE FUNCTION _rrule.before("rruleset_array" _rrule.RRULESET[], "when" TIMESTAMP)
RETURNS SETOF TSRANGE AS $$
  SELECT *
  FROM _rrule.occurrences("rruleset_array", tsrange(NULL, "when", '[]'));
$$ LANGUAGE SQL STRICT IMMUTABLE;

