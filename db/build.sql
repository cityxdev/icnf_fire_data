CREATE EXTENSION IF NOT EXISTS postgis ;

CREATE SCHEMA raw;
CREATE TABLE raw.data (
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
    nomecco integer,
    dataalerta character varying,
    horaalerta time without time zone,
    local character varying,
    concelho character varying,
    freguesia character varying,
    fontealerta character varying,
    ine integer,
    x integer,
    y integer,
    dia integer,
    mes integer,
    hora integer,
    operador character varying,
    perimetro character varying,
    aps character varying,
    causa integer,
    tipocausa character varying,
    dhinicio character varying,
    dhfim character varying,
    duracao integer,
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
    humidaderelativa integer,
    ventointensidade integer,
    ventointensidade_vetor integer,
    ventodirecao_vetor integer,
    precepitacao double precision,
    ffmc double precision,
    dmc double precision,
    dc double precision,
    isi double precision,
    bui double precision,
    fwi double precision,
    dsr double precision,
    thc double precision,
    modfarsite integer,
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

CREATE SCHEMA lau;
CREATE TABLE lau.lau(
     code varchar(6) NOT NULL PRIMARY KEY,
     name varchar(100) NOT NULL
);


CREATE SCHEMA ref;
CREATE TABLE ref.fire_type(
    id integer PRIMARY KEY ,
    code varchar(6) NOT NULL
);
CREATE TABLE ref.alarm_source(
    id integer PRIMARY KEY ,
    code varchar(6) NOT NULL
);
CREATE TABLE ref.cause(
    id integer PRIMARY KEY ,
    code varchar(6) NOT NULL
);


CREATE TABLE fire(
    id serial,
    year integer NOT NULL,
    id_rel_lau varchar(6) NOT NULL REFERENCES lau.lau(code),
    locality varchar,
    id_ref_type integer NOT NULL REFERENCES ref.fire_type(id),
    ts timestamp without time zone NOT NULL,
    reignition boolean,
    slash_and_burn boolean,
    false_alarm boolean,
    agricultural boolean,
    total_area double precision,
    brushwood_area double precision,
    agricultural_area double precision,
    inhabited_area double precision,
    cco_code varchar,
    cco_id integer,
    alarm_ts timestamp without time zone,
    id_ref_alarm_source integer NOT NULL REFERENCES ref.alarm_source(id),
    id_ref_cause integer NOT NULL REFERENCES ref.cause(id),
    extinguishing_ts timestamp without time zone,
    first_response_ts timestamp without time zone,
    temperature double precision,
    relative_humidity double precision,
    wind_speed double precision,
    wind_direction double precision,
    precipitation double precision,
    mean_height double precision,
    mean_slope double precision,
    file_url varchar,
    point geometry(Point,4326),
    multipolygon geometry(MultiPolygon,4326)
) PARTITION BY LIST(year);
CREATE OR REPLACE FUNCTION partitions()
RETURNS void AS
$$
BEGIN
    FOR y IN 2001..2050 LOOP
        EXECUTE 'CREATE TABLE fire_'||y||' PARTITION OF fire FOR VALUES IN ('||y||');';
        EXECUTE 'CREATE INDEX idx_geom_point_fire_'||y||' ON fire_'||y||' USING gist (point);';
        EXECUTE 'CREATE INDEX idx_geom_multipolygon_fire_'||y||' ON fire_'||y||' USING gist (multipolygon);';
        EXECUTE 'CREATE INDEX idx_search_fire_'||y||' ON fire_'||y||' (ts,id_rel_lau,total_area,temperature,wind_speed,relative_humidity,mean_height,mean_slope);';
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
SELECT partitions();
DROP FUNCTION partitions;
