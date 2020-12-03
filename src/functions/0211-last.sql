

CREATE OR REPLACE FUNCTION _rrule.last("rrule" _rrule.RRULE, "dtstart" TIMESTAMP, "duration" INTERVAL)
RETURNS TSRANGE AS $$
  SELECT occurrence
  FROM _rrule.occurrences("rrule", "dtstart", "duration") occurrence
  ORDER BY occurrence DESC LIMIT 1;
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.last("rrule" TEXT, "dtstart" TIMESTAMP, "duration" INTERVAL)
RETURNS TSRANGE AS $$
  SELECT _rrule.last(_rrule.rrule("rrule"), "dtstart", "duration");
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.last("rruleset" _rrule.RRULESET)
RETURNS TSRANGE AS $$
  SELECT occurrence
  FROM _rrule.occurrences("rruleset") occurrence
  ORDER BY occurrence DESC LIMIT 1;
$$ LANGUAGE SQL STRICT IMMUTABLE;

-- TODO: Ensure to check whether the range is finite. If not, we should return null
-- or something meaningful.
CREATE OR REPLACE FUNCTION _rrule.last("rruleset_array" _rrule.RRULESET[])
RETURNS SETOF TSRANGE AS $$
BEGIN
  IF (SELECT _rrule.is_finite("rruleset_array")) THEN
    RETURN QUERY SELECT occurrence
    FROM _rrule.occurrences("rruleset_array", '(,)'::TSRANGE) occurrence
    ORDER BY occurrence DESC LIMIT 1;
  ELSE
    RETURN QUERY SELECT NULL::TSRANGE;
  END IF;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

