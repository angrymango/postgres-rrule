CREATE OR REPLACE FUNCTION _rrule.jsonb_to_rruleset("input" jsonb)
RETURNS _rrule.RRULESET AS $$
DECLARE
  result _rrule.RRULESET;
BEGIN
  SELECT
    "dtstart"::TIMESTAMP,
    "dtend"::TIMESTAMP,
    _rrule.jsonb_to_rrule("rrule") "rrule",
    _rrule.jsonb_to_rrule("exrule") "exrule",
    "rdate"::TIMESTAMP[],
    "exdate"::TIMESTAMP[],
    "duration"::INTERVAL
  INTO result
  FROM jsonb_to_record("input") as x(
    "dtstart" text,
    "dtend" text,
    "rrule" jsonb,
    "exrule" jsonb,
    "rdate" text[],
    "exdate" text[],
    "duration" text
  );

  -- TODO: validate rruleset

  RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
