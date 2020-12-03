CREATE OR REPLACE FUNCTION _rrule.build_duration("dtstart" TIMESTAMP, "dtend" TIMESTAMP, "duration" INTERVAL)
RETURNS INTERVAL AS $$
    SELECT
        CASE 
            WHEN $2 IS NOT NULL THEN $2 - $1
            WHEN $3 IS NOT NULL THEN $3
            ELSE 'P0'::INTERVAL 
        END;
$$ LANGUAGE SQL IMMUTABLE;