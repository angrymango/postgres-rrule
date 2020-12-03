CREATE OR REPLACE FUNCTION _rrule.occurrences(
  "rrule" _rrule.RRULE,
  "dtstart" TIMESTAMP,
  "duration" INTERVAL
)
RETURNS SETOF TSRANGE AS $$
  WITH "starts" AS (
    SELECT "start"
    FROM _rrule.all_starts($1, $2) "start"
  ),
  "params" AS (
    SELECT
      "until",
      "interval"
    FROM _rrule.until($1, $2) "until"
    FULL OUTER JOIN _rrule.build_interval($1) "interval" ON (true)
  ),
  "generated" AS (
    SELECT generate_series("start", "until", "interval") "occurrence"
    FROM "params"
    FULL OUTER JOIN "starts" ON (true)
  ),
  "ordered" AS (
    SELECT DISTINCT "occurrence"
    FROM "generated"
    WHERE "occurrence" >= "dtstart"
    ORDER BY "occurrence"
  ),
  "tagged" AS (
    SELECT
      row_number() OVER (),
      "occurrence"
    FROM "ordered"
  )
  SELECT tsrange("occurrence", "occurrence" + $3, '[]')
  FROM "tagged"
  WHERE "row_number" <= "rrule"."count"
  OR "rrule"."count" IS NULL
  ORDER BY "occurrence";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences("rrule" _rrule.RRULE, "dtstart" TIMESTAMP, "duration" INTERVAL, "between" TSRANGE)
RETURNS SETOF TSRANGE AS $$
  SELECT "occurrence"
  FROM _rrule.occurrences("rrule", "dtstart", "duration") "occurrence"
  WHERE "occurrence" <@ "between";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences("rrule" TEXT, "dtstart" TIMESTAMP, "duration" INTERVAL, "between" TSRANGE)
RETURNS SETOF TSRANGE AS $$
  SELECT "occurrence"
  FROM _rrule.occurrences(_rrule.rrule("rrule"), "dtstart", "duration") "occurrence"
  WHERE "occurrence" <@ "between";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences(
  "rruleset" _rrule.RRULESET,
  "tsrange" TSRANGE
)
RETURNS SETOF TSRANGE AS $$
  WITH "rrules" AS (
    SELECT
      "rruleset"."dtstart",
      "rruleset"."rrule",
      _rrule.build_duration("rruleset"."dtstart", "rruleset"."dtend", "rruleset"."duration") AS "duration"
  ),
  "rdates" AS (
    SELECT _rrule.occurrences("rrule", "dtstart", "duration", "tsrange") AS "occurrence"
    FROM "rrules"
    UNION
    SELECT tsrange(o, o + "duration", '[]') AS "occurrence" FROM unnest("rruleset"."rdate") AS o, "rrules"
  ),
  "exrules" AS (
    SELECT
      "rruleset"."dtstart",
      "rruleset"."exrule",
      _rrule.build_duration("rruleset"."dtstart", "rruleset"."dtend", "rruleset"."duration") AS "duration"
  ),
  "exdates" AS (
    SELECT _rrule.occurrences("exrule", "dtstart", "duration", "tsrange") AS "occurrence"
    FROM "exrules"
    UNION
    SELECT tsrange(o, o + "duration", '[]') AS "occurrence" FROM unnest("rruleset"."exdate") AS o, "rrules"
  )
  SELECT "occurrence" FROM "rdates"
  EXCEPT
  SELECT "occurrence" FROM "exdates"
  ORDER BY "occurrence";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences("rruleset" _rrule.RRULESET)
RETURNS SETOF TSRANGE AS $$
  SELECT _rrule.occurrences("rruleset", '(,)'::TSRANGE);
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences(
  "rruleset_array" _rrule.RRULESET[],
  "tsrange" TSRANGE
  -- TODO: add a default limit and then use that limit from `first` and `last`
)
RETURNS SETOF TSRANGE AS $$
DECLARE
  i int;
  lim int;
  q text := '';
BEGIN
  lim := array_length("rruleset_array", 1);

  IF lim IS NULL THEN
    q := 'VALUES (NULL::TSRANGE) LIMIT 0;';
  ELSE
    FOR i IN 1..lim
    LOOP
      q := q || $q$SELECT _rrule.occurrences('$q$ || "rruleset_array"[i] ||$q$'::_rrule.RRULESET, '$q$ || "tsrange" ||$q$'::TSRANGE)$q$;
      IF i != lim THEN
        q := q || ' UNION ';
      END IF;
    END LOOP;
    q := q || ' ORDER BY occurrences ASC';
  END IF;

  RETURN QUERY EXECUTE q;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;