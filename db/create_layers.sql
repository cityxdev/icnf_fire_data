CREATE SCHEMA IF NOT EXISTS layers;

GRANT USAGE ON SCHEMA layers TO fire_read;

CREATE VIEW layers.fire AS
    SELECT
        f.id,
        f.original_id,
        f.year,
        f.ts,
        l3.name AS lau3,
        l2.name AS lau2,
        l1.name AS lau1,
        f.locality,
        ft.code AS fire_type,
        a.code  AS alarm_source,
        c.code  AS cause,
        ct.code AS cause_type,
        f.rekindle,
        f.slash_and_burn,
        f.agricultural,
        f.total_area,
        f.brushwood_area,
        f.agricultural_area,
        f.inhabited_area,
        f.alarm_ts,
        f.first_response_ts,
        f.extinguishing_ts,
        f.temperature,
        f.relative_humidity,
        f.wind_speed,
        f.wind_direction,
        f.precipitation,
        f.mean_height,
        f.mean_slope,
        f.point,
        f.multipolygon
    FROM fire f
        LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
        LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
        LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
        LEFT JOIN ref.fire_type ft ON f.id_ref_fire_type = ft.id
        LEFT JOIN ref.alarm_source a ON f.id_ref_alarm_source = a.id
        LEFT JOIN ref.cause c ON f.id_ref_cause = c.id
        LEFT JOIN ref.cause_type ct ON f.id_ref_cause_type = ct.id;


CREATE VIEW layers.fire_points AS
SELECT
    id,
    original_id,
    year,
    ts,
    lau3,
    lau2,
    lau1,
    locality,
    fire_type,
    alarm_source,
    cause,
    cause_type,
    rekindle,
    slash_and_burn,
    agricultural,
    total_area,
    brushwood_area,
    agricultural_area,
    inhabited_area,
    alarm_ts,
    first_response_ts,
    extinguishing_ts,
    temperature,
    relative_humidity,
    wind_speed,
    wind_direction,
    precipitation,
    mean_height,
    mean_slope,
    point
FROM layers.fire;


CREATE VIEW layers.fire_multipolygons AS
SELECT
    id,
    original_id,
    year,
    ts,
    lau3,
    lau2,
    lau1,
    locality,
    fire_type,
    alarm_source,
    cause,
    cause_type,
    rekindle,
    slash_and_burn,
    agricultural,
    total_area,
    brushwood_area,
    agricultural_area,
    inhabited_area,
    alarm_ts,
    first_response_ts,
    extinguishing_ts,
    temperature,
    relative_humidity,
    wind_speed,
    wind_direction,
    precipitation,
    mean_height,
    mean_slope,
    multipolygon
FROM layers.fire
WHERE multipolygon IS NOT NULL;


CREATE VIEW layers.lau1 AS
    SELECT
        code,
        name,
        area,
        geom
    FROM lau.lau
    WHERE level = 1;
CREATE VIEW layers.lau2 AS
    SELECT
        l2.code,
        l2.name,
        l2.area,
        l1.name AS parent_name,
        l2.geom
    FROM lau.lau l2
        JOIN lau.lau l1 ON l2.code_parent=l1.code
    WHERE l2.level = 2;
CREATE VIEW layers.lau3 AS
    SELECT
        l3.code,
        l3.name,
        l3.area,
        l1.name AS grand_parent_name,
        l2.name AS parent_name,
        l3.geom
    FROM lau.lau l3
        JOIN lau.lau l2 ON l3.code_parent=l2.code
        JOIN lau.lau l1 ON l2.code_parent=l1.code
    WHERE l3.level = 3;


-- these strange summary queries ensure we have polygons even if there are no fires
CREATE VIEW layers.lau2_summary AS
    SELECT
        s.count, s.year, s.month, s.lau2, l.geom
    FROM lau.lau l
        JOIN (
            SELECT
                count(*) AS count,
                f.year,
                EXTRACT(MONTH FROM f.ts) AS month,
                l2.name AS lau2,
                l2.code
            FROM fire f
                 LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
                 LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
            GROUP BY f.year, EXTRACT(MONTH FROM f.ts),l2.code,l2.name
        ) s ON l.code=s.code;

--Example of selecting the lau1_summary view to get a count per lau1 for year 2022
--SELECT
--    year,
--    lau1,
--    sum(count) AS count,
--    first(geom) AS geom
--FROM layers.lau1_summary
--WHERE year=2022
--GROUP BY year,lau1


CREATE VIEW layers.lau1_summary AS
    SELECT
        s.count, s.year, s.month, s.lau1, l.geom
    FROM lau.lau l
         JOIN (
            SELECT
                count(*) AS count,
                f.year,
                EXTRACT(MONTH FROM f.ts) AS month,
                l1.name AS lau1,
                l1.code
            FROM fire f
                 LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
                 LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
                 LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
            GROUP BY f.year, EXTRACT(MONTH FROM f.ts),l1.code,l1.name
         ) s ON l.code=s.code;


CREATE OR REPLACE function first_agg(anyelement, anyelement)
    returns anyelement language sql immutable strict
as $$ select $1; $$;
CREATE OR REPLACE function last_agg(anyelement, anyelement)
    returns anyelement language sql immutable strict
as $$ select $2; $$;

CREATE AGGREGATE first(anyelement) (
    sfunc = first_agg,
    stype = anyelement
);
CREATE AGGREGATE last(anyelement) (
    sfunc = last_agg,
    stype = anyelement
);

GRANT SELECT ON ALL TABLES IN SCHEMA layers TO fire_read;
