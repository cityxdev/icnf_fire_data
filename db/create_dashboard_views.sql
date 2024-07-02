CREATE SCHEMA IF NOT EXISTS dashboard;

GRANT USAGE ON SCHEMA dashboard TO fire_read;

CREATE VIEW dashboard.lau1 AS
    SELECT
        code,
        name,
        area
    FROM lau.lau
    WHERE level = 1;
CREATE VIEW dashboard.lau2 AS
    SELECT
        l2.code,
        l2.name,
        l2.area,
        l1.name AS parent_name
    FROM lau.lau l2
        JOIN lau.lau l1 ON l2.code_parent=l1.code
    WHERE l2.level = 2;
CREATE VIEW dashboard.lau3 AS
    SELECT
        l3.code,
        l3.name,
        l3.area,
        l1.name AS grand_parent_name,
        l2.name AS parent_name
    FROM lau.lau l3
        JOIN lau.lau l2 ON l3.code_parent=l2.code
        JOIN lau.lau l1 ON l2.code_parent=l1.code
    WHERE l3.level = 3;

CREATE VIEW dashboard.total_fire_yearly AS
    SELECT
        count(*) AS count,
        f.year,
        l1.code AS lau1_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE f.total_area IS NULL OR f.total_area>10
    GROUP BY f.year, l1.code;
CREATE VIEW dashboard.total_fire_monthly AS
    SELECT
        count(*) AS count,
        f.year,
        EXTRACT(MONTH FROM f.ts)::int AS month,
        l1.code AS lau1_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE f.total_area IS NULL OR f.total_area>10
    GROUP BY f.year, EXTRACT(MONTH FROM f.ts),l1.code;
CREATE VIEW dashboard.total_fire_hourly AS
    SELECT
        count(*) AS count,
        f.year,
        EXTRACT(HOUR FROM f.ts)::int AS hour,
        l1.code AS lau1_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE f.total_area IS NULL OR f.total_area>10
    GROUP BY f.year, EXTRACT(HOUR FROM f.ts),l1.code;

CREATE VIEW dashboard.total_area_yearly AS
    SELECT
        sum(f.total_area) AS total_area,
        min(f.total_area) AS min_area,
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY f.total_area) AS p25_area,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY f.total_area) AS median_area,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY f.total_area) AS p75_area,
        max(f.total_area) AS max_area,
        f.year,
        l1.code AS lau1_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE f.total_area IS NOT NULL AND f.total_area>10
    GROUP BY f.year,l1.code;
CREATE VIEW dashboard.total_area_monthly AS
    SELECT
        sum(f.total_area) AS total_area,
        f.year,
        EXTRACT(MONTH FROM f.ts)::int AS month,
        l1.code AS lau1_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE f.total_area IS NOT NULL AND f.total_area>10
    GROUP BY f.year, EXTRACT(MONTH FROM f.ts),l1.code;

CREATE VIEW dashboard.cause_type_yearly AS
    SELECT
        count(*) AS count,
        sum(f.total_area) AS area,
        f.year,
        l1.code AS lau1_code,
        ct.code AS cause_type_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
         LEFT JOIN ref.cause_type ct ON f.id_ref_cause_type = ct.id
    WHERE f.total_area IS NOT NULL AND f.total_area>10
    GROUP BY f.year,l1.code,ct.code;
CREATE VIEW dashboard.cause_type_monthly AS
    SELECT
        count(*) AS count,
        sum(f.total_area) AS area,
        f.year,
        EXTRACT(MONTH FROM f.ts)::int AS month,
        l1.code AS lau1_code,
        ct.code AS cause_type_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
         LEFT JOIN ref.cause_type ct ON f.id_ref_cause_type = ct.id
    WHERE f.total_area IS NOT NULL AND f.total_area>10
    GROUP BY f.year,EXTRACT(MONTH FROM f.ts),l1.code,ct.code;
CREATE VIEW dashboard.cause_type_hourly AS
    SELECT
        count(*) AS count,
        sum(f.total_area) AS area,
        f.year,
        EXTRACT(HOUR FROM f.ts)::int AS hour,
        l1.code AS lau1_code,
        ct.code AS cause_type_code
    FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
         LEFT JOIN ref.cause_type ct ON f.id_ref_cause_type = ct.id
    WHERE f.total_area IS NOT NULL AND f.total_area>10
    GROUP BY f.year,EXTRACT(HOUR FROM f.ts),l1.code,ct.code;

CREATE OR REPLACE VIEW dashboard.response_time AS
WITH stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS p75,
        l1.code AS lau1_code,
        f.year
    FROM fire f
             LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
             LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
             LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE (f.total_area IS NULL OR f.total_area>10)
      AND f.first_response_ts IS NOT NULL
      AND f.alarm_ts IS NOT NULL
      AND f.first_response_ts>f.alarm_ts
    GROUP BY f.year,l1.code
)
SELECT
    min(f.time) AS min,
    PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY f.time) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY f.time) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY f.time) AS p75,
    max(f.time) AS max,
    f.lau1_code,
    f.year
FROM (
         SELECT
                 EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts)*1000 AS time,
                 f.year,
                 l1.code AS lau1_code
         FROM fire f
                  LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
                  LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
                  LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
         WHERE (f.total_area IS NULL OR f.total_area>10)
           AND f.first_response_ts IS NOT NULL
           AND f.alarm_ts IS NOT NULL
           AND f.first_response_ts>f.alarm_ts
     ) f
         JOIN stats s ON s.year=f.year AND s.lau1_code=f.lau1_code
WHERE f.time BETWEEN s.p25-1.5*(s.p75-s.p25) AND s.p75+1.5*(s.p75-s.p25)
GROUP BY f.year,f.lau1_code;
CREATE OR REPLACE VIEW dashboard.firefighting_duration AS
WITH stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS p75,
        l1.code AS lau1_code,
        f.year
    FROM fire f
             LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
             LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
             LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE (f.total_area IS NULL OR f.total_area>10)
      AND f.extinguishing_ts IS NOT NULL
      AND f.first_response_ts IS NOT NULL
      AND f.extinguishing_ts>f.first_response_ts
    GROUP BY f.year,l1.code
)
SELECT
    min(f.time) AS min,
    PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY f.time) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY f.time) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY f.time) AS p75,
    max(f.time) AS max,
    f.lau1_code,
    f.year
FROM (
         SELECT
                 EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts)*1000 AS time,
                 f.year,
                 l1.code AS lau1_code
         FROM fire f
                  LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
                  LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
                  LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
         WHERE (f.total_area IS NULL OR f.total_area>10)
           AND f.extinguishing_ts IS NOT NULL
           AND f.first_response_ts IS NOT NULL
           AND f.extinguishing_ts>f.first_response_ts
     ) f
         JOIN stats s ON s.year=f.year AND s.lau1_code=f.lau1_code
WHERE f.time BETWEEN s.p25-1.5*(s.p75-s.p25) AND 60::bigint*24*60*60*1000
GROUP BY f.year,f.lau1_code;
CREATE OR REPLACE VIEW dashboard.total_duration AS
WITH stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS p75,
        l1.code AS lau1_code,
        f.year
    FROM fire f
             LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
             LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
             LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
    WHERE (f.total_area IS NULL OR f.total_area>10)
      AND f.extinguishing_ts IS NOT NULL
      AND f.alarm_ts IS NOT NULL
      AND f.extinguishing_ts>f.alarm_ts
    GROUP BY f.year,l1.code
)
SELECT
    min(f.time) AS min,
    PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY f.time) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY f.time) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY f.time) AS p75,
    max(f.time) AS max,
    f.lau1_code,
    f.year
FROM (
         SELECT
                 EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts)*1000 AS time,
                 f.year,
                 l1.code AS lau1_code
         FROM fire f
                  LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
                  LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
                  LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
         WHERE (f.total_area IS NULL OR f.total_area>10)
           AND f.extinguishing_ts IS NOT NULL
           AND f.alarm_ts IS NOT NULL
           AND f.extinguishing_ts>f.alarm_ts
     ) f
         JOIN stats s ON s.year=f.year AND s.lau1_code=f.lau1_code
WHERE f.time BETWEEN s.p25-1.5*(s.p75-s.p25) AND 60::bigint*24*60*60*1000
GROUP BY f.year,f.lau1_code;


GRANT SELECT ON ALL TABLES IN SCHEMA dashboard TO fire_read;
