INSERT INTO lau.lau(code,name,level,geom,area)
SELECT
    dicofre AS code,
    freguesia AS name,
    3 AS level,
    ST_Transform(ST_Multi(geom),4326)::geography AS geom,
    ST_Area(geom) AS area
FROM (
     SELECT dicofre,ST_Union(wkb_geometry) AS geom, freguesia
     FROM lau
     GROUP BY dicofre, freguesia
) a;

INSERT INTO lau.lau(code,name,level,geom,area)
SELECT
    dicofre AS code,
    concelho AS name,
    2 AS level,
    ST_Transform(ST_Multi(geom),4326)::geography AS geom,
    ST_Area(geom) AS area
FROM (
    SELECT substring(dicofre from 0 for 5) AS dicofre, ST_Union(wkb_geometry) AS geom, concelho
    FROM lau
    GROUP BY substring(dicofre from 0 for 5), concelho
) a;
UPDATE lau.lau
SET code_parent = substring(code from 0 for 5)
WHERE level=3;

INSERT INTO lau.lau(code,name,level,geom,area)
SELECT
    dicofre AS code,
    distrito AS name,
    1 AS level,
    ST_Transform(ST_Multi(geom),4326)::geography AS geom,
    ST_Area(geom) AS area
FROM (
     SELECT substring(dicofre from 0 for 3) AS dicofre, ST_Union(wkb_geometry) AS geom, distrito
     FROM lau
     GROUP BY substring(dicofre from 0 for 3), distrito
) a;
UPDATE lau.lau
SET code_parent = substring(code from 0 for 3)
WHERE level=2;


DROP TABLE lau;