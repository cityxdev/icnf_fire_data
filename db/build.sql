CREATE EXTENSION IF NOT EXISTS postgis ;

CREATE SCHEMA raw;
CREATE TABLE raw.data (
      id serial PRIMARY KEY,
      original_id character varying,
      created_ts timestamp without time zone NOT NULL DEFAULT now(),
      updated_ts timestamp without time zone NOT NULL DEFAULT now(),
      distrito character varying,
      tipo character varying,
      ano integer,
      areapov double precision,
      areamato double precision,
      areaagric double precision,
      areatotal double precision,
      reacendimentos integer,
      queimada integer,
      falsoalarme integer,
      fogacho integer,
      incendio integer,
      agricola integer,
      ncco bigint,
      nomecco character varying,
      dataalerta character varying,
      horaalerta time without time zone,
      local character varying,
      concelho character varying,
      freguesia character varying,
      fontealerta character varying,
      ine double precision,
      x double precision,
      y double precision,
      dia integer,
      mes integer,
      hora integer,
      operador character varying,
      perimetro character varying,
      aps character varying,
      causa double precision,
      tipocausa character varying,
      dhinicio character varying,
      dhfim character varying,
      duracao double precision,
      hahora double precision,
      dataextincao character varying,
      horaextincao time without time zone,
      data1intervencao character varying,
      hora1intervencao time without time zone,
      queima integer,
      lat double precision,
      lon double precision,
      causafamilia character varying,
      temperatura double precision,
      humidaderelativa double precision,
      ventointensidade double precision,
      ventointensidade_vetor double precision,
      ventodirecao_vetor double precision,
      precepitacao double precision,
      ffmc double precision,
      dmc double precision,
      dc double precision,
      isi double precision,
      bui double precision,
      fwi double precision,
      dsr double precision,
      thc character varying,
      modfarsite double precision,
      altitudemedia double precision,
      declivemedio double precision,
      horasexposicaomedia character varying,
      dendidaderv double precision,
      cosn5variedade double precision,
      areamanchamodfarsite double precision,
      areasficheiros_gnr character varying,
      areasficheiros_gtf character varying,
      ficheiroimagem_gnr character varying,
      areasficheiroshp_gtf character varying,
      areasficheiroshpxml_gtf character varying,
      areasficheirodbf_gtf character varying,
      areasficheiroprj_gtf character varying,
      areasficheirosbn_gtf character varying,
      areasficheirosbx_gtf character varying,
      areasficheiroshx_gtf character varying,
      areasficheirozip_saa character varying
);
CREATE INDEX idx_data_ano_mes_dia ON raw.data(ano,mes,dia);
CREATE OR REPLACE FUNCTION raw.update_ts()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_ts = now();
RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER update_data_updatedts BEFORE UPDATE ON raw.data FOR EACH ROW EXECUTE PROCEDURE raw.update_ts();

CREATE SCHEMA lau;
CREATE TABLE lau.lau(
     code varchar(6) NOT NULL PRIMARY KEY,
     code_parent varchar(6),
     name varchar(150) NOT NULL,
     geom geography(MultiPolygon,4326) NOT NULL,
     area double precision NOT NULL,
     level integer NOT NULL
);
CREATE UNIQUE INDEX idx_un_code_lau ON lau.lau(code);
CREATE INDEX idx_geom_lau ON lau.lau USING gist (geom);


CREATE SCHEMA ref;
CREATE TABLE ref.fire_type(
    id serial PRIMARY KEY ,
    code varchar(60),
    raw_name varchar
);
CREATE UNIQUE INDEX idx_un_raw_name_fire_type ON ref.fire_type(raw_name);
CREATE TABLE ref.alarm_source(
    id serial PRIMARY KEY ,
    code varchar(60),
    raw_name varchar
);
CREATE UNIQUE INDEX idx_un_raw_name_alarm_source ON ref.alarm_source(raw_name);
CREATE TABLE ref.cause(
    id serial PRIMARY KEY ,
    code varchar(60),
    raw_name varchar
);
CREATE UNIQUE INDEX idx_un_raw_name_cause ON ref.cause(raw_name);
CREATE TABLE ref.cause_type(
    id serial PRIMARY KEY ,
    code varchar(60),
    raw_name varchar
);
CREATE UNIQUE INDEX idx_un_raw_name_cause_type ON ref.cause_type(raw_name);


CREATE TABLE fire(
    id serial NOT NULL,
    original_id character varying NOT NULL,
    id_rel_raw_data integer NOT NULL,
    created_ts timestamp without time zone NOT NULL,
    updated_ts timestamp without time zone NOT NULL DEFAULT now(),
    year integer NOT NULL,
    id_rel_lau varchar(6) NOT NULL REFERENCES lau.lau(code),
    locality varchar,
    id_ref_fire_type integer NOT NULL REFERENCES ref.fire_type(id),
    ts timestamp without time zone NOT NULL,
    rekindle boolean,
    slash_and_burn boolean,
    agricultural boolean,
    total_area double precision,
    brushwood_area double precision,
    agricultural_area double precision,
    inhabited_area double precision,
    cco_code varchar,
    cco_id varchar,
    alarm_ts timestamp without time zone,
    id_ref_alarm_source integer REFERENCES ref.alarm_source(id),
    id_ref_cause integer REFERENCES ref.cause(id),
    id_ref_cause_type integer REFERENCES ref.cause_type(id),
    extinguishing_ts timestamp without time zone,
    first_response_ts timestamp without time zone,
    temperature double precision,
    relative_humidity double precision,
    wind_speed double precision,
    wind_direction double precision,
    precipitation double precision,
    mean_height double precision,
    mean_slope double precision,
    file_urls varchar,
    point geography(Point,4326),
    multipolygon geography(MultiPolygon,4326)
) PARTITION BY LIST(year);

CREATE OR REPLACE FUNCTION update_ts()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_ts = now();
RETURN NEW;
END;
$$ language 'plpgsql';


CREATE OR REPLACE FUNCTION partitions()
RETURNS void AS
$$
BEGIN
    FOR y IN 2001..2050 LOOP
        EXECUTE 'CREATE TABLE fire_'||y||' PARTITION OF fire FOR VALUES IN ('||y||');';
        EXECUTE 'CREATE INDEX idx_geom_point_fire_'||y||' ON fire_'||y||' USING gist (point);';
        EXECUTE 'CREATE INDEX idx_geom_multipolygon_fire_'||y||' ON fire_'||y||' USING gist (multipolygon);';
        EXECUTE 'CREATE INDEX idx_search_fire_'||y||' ON fire_'||y||' (ts,alarm_ts,extinguishing_ts,first_response_ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);';
        EXECUTE 'CREATE TRIGGER update_fire_'||y||'_updatedts BEFORE UPDATE ON fire_'||y||' FOR EACH ROW EXECUTE PROCEDURE update_ts()';
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
SELECT partitions();
DROP FUNCTION partitions;


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

GRANT SELECT ON ALL TABLES IN SCHEMA public TO fire_read;
