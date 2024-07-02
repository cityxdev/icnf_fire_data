ALTER VIEW dashboard.total_fire_montlhy RENAME TO total_fire_monthly;

DROP VIEW dashboard.cause_type_yearly;
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


DROP VIEW dashboard.cause_type_monthly;
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


DROP VIEW dashboard.cause_type_hourly;
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


DROP VIEW dashboard.total_area_yearly;
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



DROP VIEW dashboard.response_time;
CREATE VIEW dashboard.response_time AS
SELECT
        max(EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS max,
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS p75,
        min(EXTRACT(EPOCH FROM f.first_response_ts-f.alarm_ts))*1000 AS min,
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
GROUP BY f.year,l1.code;


DROP VIEW dashboard.firefighting_duration;
CREATE VIEW dashboard.firefighting_duration AS
SELECT
        max(EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS max,
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS p75,
        min(EXTRACT(EPOCH FROM f.extinguishing_ts-f.first_response_ts))*1000 AS min,
        l1.code AS lau1_code,
        f.year
FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
WHERE (f.total_area IS NULL OR f.total_area>10)
  AND f.first_response_ts IS NOT NULL
  AND f.extinguishing_ts IS NOT NULL
  AND f.extinguishing_ts>f.first_response_ts
GROUP BY f.year,l1.code;


DROP VIEW dashboard.total_duration;
CREATE VIEW dashboard.total_duration AS
SELECT
        max(EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS max,
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS p75,
        min(EXTRACT(EPOCH FROM f.extinguishing_ts-f.alarm_ts))*1000 AS min,
        l1.code AS lau1_code,
        f.year
FROM fire f
         LEFT JOIN lau.lau l3 ON f.id_rel_lau = l3.code
         LEFT JOIN lau.lau l2 ON l2.code = l3.code_parent
         LEFT JOIN lau.lau l1 ON l1.code = l2.code_parent
WHERE (f.total_area IS NULL OR f.total_area>10)
  AND f.alarm_ts IS NOT NULL
  AND f.extinguishing_ts IS NOT NULL
  AND f.extinguishing_ts>f.alarm_ts
GROUP BY f.year,l1.code;


GRANT SELECT ON ALL TABLES IN SCHEMA dashboard TO fire_read;



ALTER TABLE fire ADD COLUMN original_id character varying;
UPDATE FIRE
    SET original_id=rd.original_id
    FROM raw.data rd
    WHERE id_rel_raw_data=rd.id;
ALTER TABLE fire ALTER COLUMN original_id SET NOT NULL;


DROP VIEW layers.fire CASCADE;
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

DROP VIEW IF EXISTS layers.fire_points CASCADE;
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

DROP VIEW IF EXISTS layers.fire_multipolygons CASCADE;
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


DROP VIEW IF EXISTS layers.lau2_summary CASCADE;
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


DROP VIEW IF EXISTS layers.lau1_summary CASCADE;
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

CREATE OR REPLACE FUNCTION process_raw_data(year_from integer, month_from integer, day_from integer)
    RETURNS void AS
$$
DECLARE
    ts_from timestamp without time zone;
    ts_from_str varchar;
BEGIN
    INSERT INTO ref.alarm_source(raw_name)
    SELECT distinct lower(fontealerta)
    FROM raw.data rd
    WHERE (year_from IS NULL OR rd.ano::integer>=year_from) AND (month_from IS NULL OR rd.mes::integer>=month_from) AND (day_from IS NULL OR rd.dia::integer>=day_from)
      AND fontealerta IS NOT NULL AND not exists(SELECT id FROM ref.alarm_source r WHERE lower(r.raw_name)=lower(rd.fontealerta));
    INSERT INTO ref.fire_type(raw_name)
    SELECT distinct lower(tipo)
    FROM raw.data rd
    WHERE (year_from IS NULL OR rd.ano::integer>=year_from) AND (month_from IS NULL OR rd.mes::integer>=month_from) AND (day_from IS NULL OR rd.dia::integer>=day_from)
      AND tipo IS NOT NULL AND not exists(SELECT id FROM ref.fire_type r WHERE lower(r.raw_name)=lower(rd.tipo));
    INSERT INTO ref.cause(raw_name)
    SELECT distinct lower(causafamilia)
    FROM raw.data rd
    WHERE (year_from IS NULL OR rd.ano::integer>=year_from) AND (month_from IS NULL OR rd.mes::integer>=month_from) AND (day_from IS NULL OR rd.dia::integer>=day_from)
      AND causafamilia IS NOT NULL AND not exists(SELECT id FROM ref.cause r WHERE lower(r.raw_name)=lower(rd.causafamilia));
    INSERT INTO ref.cause_type(raw_name)
    SELECT distinct lower(tipocausa)
    FROM raw.data rd
    WHERE (year_from IS NULL OR rd.ano::integer>=year_from) AND (month_from IS NULL OR rd.mes::integer>=month_from) AND (day_from IS NULL OR rd.dia::integer>=day_from)
      AND tipocausa IS NOT NULL AND not exists(SELECT id FROM ref.cause_type r WHERE lower(r.raw_name)=lower(rd.tipocausa));

    IF year_from IS NOT NULL THEN
        ts_from_str:=year_from||'-';
    END IF;
    IF month_from IS NOT NULL THEN
        ts_from_str:=ts_from_str||month_from||'-';
    ELSE
        ts_from_str:=ts_from_str||'01'||'-';
    END IF;
    IF day_from IS NOT NULL THEN
        ts_from_str:=ts_from_str||day_from;
    ELSE
        ts_from_str:=ts_from_str||'01';
    END IF;
    ts_from:=ts_from_str::timestamp without time zone;
    DELETE FROM fire WHERE ts>=ts_from;

    INSERT INTO fire (id_rel_raw_data,original_id,created_ts,year,ts,id_ref_fire_type,id_rel_lau,locality,rekindle,slash_and_burn,agricultural,
                      total_area,brushwood_area,agricultural_area,inhabited_area,cco_code,cco_id,alarm_ts,id_ref_alarm_source,
                      id_ref_cause,id_ref_cause_type,extinguishing_ts,first_response_ts,temperature,relative_humidity,wind_speed,
                      wind_direction,precipitation,mean_height,mean_slope,file_urls,point)
    SELECT
        rd.id AS id_rel_raw_data,
        rd.original_id AS original_id,
        now() AS created_ts,
        rd.ano::integer AS year,
        (rd.ano||'-'||rd.mes||'-'||rd.dia||' '||rd.hora||':00:00')::timestamp without time zone AS ts,
        CASE WHEN rd.tipo IS NOT NULL THEN (SELECT id FROM ref.fire_type WHERE raw_name = lower(rd.tipo)) ELSE null END AS id_ref_fire_type,
        l.code AS id_rel_lau,
        rd.local AS locality,
        reacendimentos=1 AS rekindle,
        queimada=1 AS slash_and_burn,
        agricola=1 AS agricultural,
        areatotal*10000 AS total_area,
        areamato*10000 AS brushwood_area,
        areaagric*10000 AS agricultural_area,
        areapov*10000 AS inhabited_area,
        nomecco AS cco_code,
        ncco::varchar AS cco_id,
        (to_date(dataalerta,'DD-MM-YYYY')||' '||horaalerta)::timestamp without time zone AS alarm_ts,
        CASE WHEN rd.fontealerta IS NOT NULL THEN (SELECT id FROM ref.alarm_source WHERE raw_name = lower(rd.fontealerta)) ELSE null END AS id_ref_alarm_source,
        CASE WHEN rd.causafamilia IS NOT NULL THEN (SELECT id FROM ref.cause WHERE raw_name = lower(rd.causafamilia)) ELSE null END AS id_ref_cause,
        CASE WHEN rd.tipocausa IS NOT NULL THEN (SELECT id FROM ref.cause_type WHERE raw_name = lower(rd.tipocausa)) ELSE null END AS id_ref_cause_type,
        (to_date(dataextincao,'DD-MM-YYYY')||' '||horaextincao)::timestamp without time zone AS extinguishing_ts,
        (to_date(data1intervencao,'DD-MM-YYYY')||' '||hora1intervencao)::timestamp without time zone AS first_response_ts,
        temperatura AS temperature,
        humidaderelativa AS relative_humidity,
        ventointensidade AS wind_speed,
        ventodirecao_vetor AS wind_direction,
        CASE WHEN precepitacao = 1.734723e-15 THEN NULL ELSE precepitacao END AS precipitation,
        altitudemedia AS mean_height,
        declivemedio AS mean_slope,
        nullif(trim(concat_ws(',',
                              areasficheiros_gnr,
                              areasficheiros_gtf,
                              areasficheiroshp_gtf,
                              areasficheiroshpxml_gtf,
                              areasficheirodbf_gtf,
                              areasficheiroprj_gtf,
                              areasficheirosbn_gtf,
                              areasficheirosbx_gtf,
                              areasficheiroshx_gtf,
                              areasficheirozip_saa
                    )), '') AS file_urls,
        ST_SetSRID(ST_MakeValid(ST_SnapToGrid(ST_Point(rd.lon, rd.lat),0.00001)), 4326)::geography AS point
    FROM raw.data rd
             JOIN lau.lau l ON l.level=3 AND ST_Intersects(ST_SetSRID(ST_Point(rd.lon, rd.lat), 4326),l.geom)
    WHERE year_from IS NULL
       OR ((rd.ano::integer>=year_from)
        AND (rd.ano||'-'||rd.mes||'-'||rd.dia)::timestamp without time zone >= ts_from);

    UPDATE ref.cause SET code = 'UNDETERMINED' WHERE raw_name = 'indeterminadas';
    UPDATE ref.cause SET code = 'ACCIDENTAL_OTHERS' WHERE raw_name = 'acidentais - outros';
    UPDATE ref.cause SET code = 'ACCIDENTAL_MACHINERY' WHERE raw_name = 'acidentais - maquinaria';
    UPDATE ref.cause SET code = 'ACCIDENTAL_TRANSPORT_COMMUNICATION' WHERE raw_name = 'acidentais - transportes e comunicações';
    UPDATE ref.cause SET code = 'ARSON_IMPUTABLE' WHERE raw_name = 'incendiarismo - imputáveis';
    UPDATE ref.cause SET code = 'ARSON_UNKNOWN_MOTIVATION' WHERE raw_name = 'incendiarismo - sem motivação conhecida';
    UPDATE ref.cause SET code = 'ARSON_UNIMPUTABLE' WHERE raw_name = 'incendiarismo - inimputáveis';
    UPDATE ref.cause SET code = 'FIRE_USE_EXTENSIVE_BURN_FOREST_AND_AGRICULTURAL' WHERE raw_name = 'queimadas de sobrantes florestais ou agrícolas';
    UPDATE ref.cause SET code = 'FIRE_USE_EXTENSIVE_BURN_PASTURE_MANAGEMENT' WHERE raw_name = 'queimadas para gestão de pasto para gado';
    UPDATE ref.cause SET code = 'FIRE_USE_BONFIRE' WHERE raw_name = 'uso do fogo - fogueiras';
    UPDATE ref.cause SET code = 'FIRE_USE_FIREWORK' WHERE raw_name = 'uso do fogo - lançamento foguetes';
    UPDATE ref.cause SET code = 'FIRE_USE_SMOKING' WHERE raw_name = 'uso do fogo - fumar';
    UPDATE ref.cause SET code = 'FIRE_USE_WASTE_BURNING_GARBAGE' WHERE raw_name = 'uso do fogo - queima de lixo';
    UPDATE ref.cause SET code = 'FIRE_USE_WASTE_BURNING_FOREST_AND_AGRICULTURAL' WHERE raw_name = 'queimas amontoados de sobrantes florestais ou agrícolas';
    UPDATE ref.cause SET code = 'FIRE_USE_OTHERS' WHERE raw_name = 'uso do fogo - outros';
    UPDATE ref.cause SET code = 'REKINDLE' WHERE raw_name = 'reacendimentos';
    UPDATE ref.cause SET code = 'STRUCTURAL_LAND_USE' WHERE raw_name = 'estruturais - uso do solo';
    UPDATE ref.cause SET code = 'STRUCTURAL_OTHERS' WHERE raw_name = 'estruturais - outras';
    UPDATE ref.cause SET code = 'STRUCTURAL_HUNTING_AND_WILDLIFE' WHERE raw_name = 'estruturais - caça e vida selvagem';
    UPDATE ref.cause SET code = 'NATURAL' WHERE raw_name = 'naturais';

    UPDATE ref.fire_type SET code = 'WASTE_BURNING' WHERE raw_name = 'queima';
    UPDATE ref.fire_type SET code = 'AGRICULTURAL' WHERE raw_name = 'agrícola';
    UPDATE ref.fire_type SET code = 'FOREST' WHERE raw_name = 'florestal';

    UPDATE ref.cause_type SET code = 'NATURAL' WHERE raw_name = 'natural';
    UPDATE ref.cause_type SET code = 'UNKNOWN' WHERE raw_name = 'desconhecida';
    UPDATE ref.cause_type SET code = 'DELIBERATE' WHERE raw_name = 'intencional';
    UPDATE ref.cause_type SET code = 'REKINDLE' WHERE raw_name = 'reacendimento';
    UPDATE ref.cause_type SET code = 'NEGLIGENT' WHERE raw_name = 'negligente';

    UPDATE ref.alarm_source SET code = 'AIR_SURVEILLANCE' WHERE raw_name = 'vig. aérea';
    UPDATE ref.alarm_source SET code = 'OTHERS' WHERE raw_name = 'outros';
    UPDATE ref.alarm_source SET code = 'GNR' WHERE raw_name = 'gnr';
    UPDATE ref.alarm_source SET code = 'PSP' WHERE raw_name = 'psp';
    UPDATE ref.alarm_source SET code = 'CDOS' WHERE raw_name = 'cdos';
    UPDATE ref.alarm_source SET code = 'CM' WHERE raw_name = 'cm';
    UPDATE ref.alarm_source SET code = 'PJ' WHERE raw_name = 'pj';
    UPDATE ref.alarm_source SET code = 'CODU' WHERE raw_name = 'codu';
    UPDATE ref.alarm_source SET code = 'POPULATION' WHERE raw_name = 'populares';
    UPDATE ref.alarm_source SET code = 'VT' WHERE raw_name = 'vt';
    UPDATE ref.alarm_source SET code = 'VMT_AGRIS' WHERE raw_name = 'vmt/agris';
    UPDATE ref.alarm_source SET code = 'PNPG' WHERE raw_name = 'pnpg';
    UPDATE ref.alarm_source SET code = 'CNGF' WHERE raw_name = 'cngf';
    UPDATE ref.alarm_source SET code = 'PRIVATE' WHERE raw_name = 'particular';
    UPDATE ref.alarm_source SET code = 'DRA' WHERE raw_name = 'dra';
    UPDATE ref.alarm_source SET code = 'PART' WHERE raw_name = 'part';
    UPDATE ref.alarm_source SET code = 'CIVIL_PROTECTION' WHERE raw_name = 's.m.prot. civil';
    UPDATE ref.alarm_source SET code = 'PROFESSIONAL_FIREFIGHTERS' WHERE raw_name = 'sapadores';
    UPDATE ref.alarm_source SET code = 'PROFESSIONAL_FOREST_FIREFIGHTERS' WHERE raw_name = 'sapadores flo';
    UPDATE ref.alarm_source SET code = 'BAV' WHERE raw_name = 'bav';
    UPDATE ref.alarm_source SET code = 'CCO' WHERE raw_name = 'cco';
    UPDATE ref.alarm_source SET code = 'BT' WHERE raw_name = 'bt';
    UPDATE ref.alarm_source SET code = 'P_MUN' WHERE raw_name = 'p.mun.';
    UPDATE ref.alarm_source SET code = 'CB' WHERE raw_name = 'cb';
    UPDATE ref.alarm_source SET code = 'GROUND_MOBILE_SURVEILLANCE' WHERE raw_name = 'vig. movel terr';
    UPDATE ref.alarm_source SET code = 'EMERGENCY_TELEPHONE_NR' WHERE raw_name = '112';
    UPDATE ref.alarm_source SET code = 'FIRE_EMERGENCY_TELEPHONE_NR' WHERE raw_name = '117';
    UPDATE ref.alarm_source SET code = 'PV' WHERE raw_name = 'pv';
    UPDATE ref.alarm_source SET code = 'BRISA' WHERE raw_name = 'brisa';
    UPDATE ref.alarm_source SET code = 'PNM' WHERE raw_name = 'pnm';
END;
$$ LANGUAGE PLPGSQL;


DROP VIEW dashboard.response_time CASCADE;
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

DROP VIEW dashboard.firefighting_duration CASCADE;
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

DROP VIEW dashboard.total_duration CASCADE;
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


GRANT SELECT ON ALL TABLES IN SCHEMA layers TO fire_read;
GRANT SELECT ON ALL TABLES IN SCHEMA dashboard TO fire_read;



DROP INDEX idx_search_fire_2001; CREATE INDEX idx_search_fire_2001 ON fire_2001 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2002; CREATE INDEX idx_search_fire_2002 ON fire_2002 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2003; CREATE INDEX idx_search_fire_2003 ON fire_2003 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2004; CREATE INDEX idx_search_fire_2004 ON fire_2004 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2005; CREATE INDEX idx_search_fire_2005 ON fire_2005 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2006; CREATE INDEX idx_search_fire_2006 ON fire_2006 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2007; CREATE INDEX idx_search_fire_2007 ON fire_2007 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2008; CREATE INDEX idx_search_fire_2008 ON fire_2008 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2009; CREATE INDEX idx_search_fire_2009 ON fire_2009 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2010; CREATE INDEX idx_search_fire_2010 ON fire_2010 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2011; CREATE INDEX idx_search_fire_2011 ON fire_2011 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2012; CREATE INDEX idx_search_fire_2012 ON fire_2012 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2013; CREATE INDEX idx_search_fire_2013 ON fire_2013 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2014; CREATE INDEX idx_search_fire_2014 ON fire_2014 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2015; CREATE INDEX idx_search_fire_2015 ON fire_2015 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2016; CREATE INDEX idx_search_fire_2016 ON fire_2016 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2017; CREATE INDEX idx_search_fire_2017 ON fire_2017 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2018; CREATE INDEX idx_search_fire_2018 ON fire_2018 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2019; CREATE INDEX idx_search_fire_2019 ON fire_2019 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2020; CREATE INDEX idx_search_fire_2020 ON fire_2020 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2021; CREATE INDEX idx_search_fire_2021 ON fire_2021 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2022; CREATE INDEX idx_search_fire_2022 ON fire_2022 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2023; CREATE INDEX idx_search_fire_2023 ON fire_2023 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2024; CREATE INDEX idx_search_fire_2024 ON fire_2024 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2025; CREATE INDEX idx_search_fire_2025 ON fire_2025 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2026; CREATE INDEX idx_search_fire_2026 ON fire_2026 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2027; CREATE INDEX idx_search_fire_2027 ON fire_2027 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2028; CREATE INDEX idx_search_fire_2028 ON fire_2028 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2029; CREATE INDEX idx_search_fire_2029 ON fire_2029 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2030; CREATE INDEX idx_search_fire_2030 ON fire_2030 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2031; CREATE INDEX idx_search_fire_2031 ON fire_2031 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2032; CREATE INDEX idx_search_fire_2032 ON fire_2032 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2033; CREATE INDEX idx_search_fire_2033 ON fire_2033 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2034; CREATE INDEX idx_search_fire_2034 ON fire_2034 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2035; CREATE INDEX idx_search_fire_2035 ON fire_2035 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2036; CREATE INDEX idx_search_fire_2036 ON fire_2036 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2037; CREATE INDEX idx_search_fire_2037 ON fire_2037 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2038; CREATE INDEX idx_search_fire_2038 ON fire_2038 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2039; CREATE INDEX idx_search_fire_2039 ON fire_2039 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2040; CREATE INDEX idx_search_fire_2040 ON fire_2040 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2041; CREATE INDEX idx_search_fire_2041 ON fire_2041 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2042; CREATE INDEX idx_search_fire_2042 ON fire_2042 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2043; CREATE INDEX idx_search_fire_2043 ON fire_2043 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2044; CREATE INDEX idx_search_fire_2044 ON fire_2044 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2045; CREATE INDEX idx_search_fire_2045 ON fire_2045 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2046; CREATE INDEX idx_search_fire_2046 ON fire_2046 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2047; CREATE INDEX idx_search_fire_2047 ON fire_2047 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2048; CREATE INDEX idx_search_fire_2048 ON fire_2048 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2049; CREATE INDEX idx_search_fire_2049 ON fire_2049 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);
DROP INDEX idx_search_fire_2050; CREATE INDEX idx_search_fire_2050 ON fire_2050 (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);

