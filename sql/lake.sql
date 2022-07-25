--aws s3 sync docs/data/log s3://compass.uol.bootcamp/sql

-- https://docs.aws.amazon.com/athena/latest/ug/create-database.html
CREATE DATABASE csvms;

-- https://docs.aws.amazon.com/athena/latest/ug/create-table.html
CREATE EXTERNAL TABLE csvms.raw_lista_frutas(
    op string, 
    op_ts timestamp,
    nm_fruta string,
    tp_fruta string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/sql/default.lista_frutas/';

--https://docs.aws.amazon.com/athena/latest/ug/create-table.html
CREATE EXTERNAL TABLE csvms.raw_tipo_frutas(
    op string, 
    op_ts timestamp,
    tp_fruta string,
    vl_fruta float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/sql/default.tipo_frutas/';

--https://docs.aws.amazon.com/athena/latest/ug/create-table.html
CREATE EXTERNAL TABLE csvms.raw_venda_frutas(
    op string, 
    op_ts timestamp,
    cod_venda integer,
    nm_fruta string,
    qtd_venda integer)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/sql/default.venda_frutas/';

-- https://docs.aws.amazon.com/athena/latest/ug/create-view.html
CREATE OR REPLACE VIEW csvms.lista_frutas
    AS
  WITH current as (
SELECT max(op_ts) last
     , nm_fruta
  FROM raw_lista_frutas
 GROUP BY nm_fruta)
SELECT raw_lista_frutas.nm_fruta
     , raw_lista_frutas.tp_fruta
  FROM raw_lista_frutas
  JOIN current 
    ON current.last = raw_lista_frutas.op_ts 
   AND current.nm_fruta = raw_lista_frutas.nm_fruta
 WHERE raw_lista_frutas.op <> 'D';

-- https://docs.aws.amazon.com/athena/latest/ug/create-view.html
CREATE OR REPLACE VIEW csvms.tipo_frutas
    AS
  WITH current as (
SELECT max(op_ts) last
     , tp_fruta
  FROM raw_tipo_frutas
 GROUP BY tp_fruta)
SELECT raw_tipo_frutas.tp_fruta
     , raw_tipo_frutas.vl_fruta
  FROM raw_tipo_frutas
  JOIN current 
    ON current.last = raw_tipo_frutas.op_ts 
   AND current.tp_fruta = raw_tipo_frutas.tp_fruta
 WHERE raw_tipo_frutas.op <> 'D';

-- https://docs.aws.amazon.com/athena/latest/ug/create-view.html
CREATE OR REPLACE VIEW csvms.venda_frutas
    AS
  WITH current as (
SELECT max(op_ts) last
     , cod_venda
  FROM raw_venda_frutas
 GROUP BY cod_venda)
SELECT raw_venda_frutas.cod_venda
     , raw_venda_frutas.nm_fruta
     , raw_venda_frutas.qtd_venda
  FROM raw_venda_frutas
  JOIN current 
    ON current.last = raw_venda_frutas.op_ts 
   AND current.cod_venda = raw_venda_frutas.cod_venda
 WHERE raw_venda_frutas.op <> 'D';

-- https://docs.aws.amazon.com/pt_br/athena/latest/ug/presto-functions.html
SELECT t.tp_fruta tipo
     , sum(if(v.qtd_venda IS NULL,0,v.qtd_venda) * t.vl_fruta) total
  FROM venda_frutas v
  RIGHT OUTER JOIN lista_frutas l ON v.nm_fruta = l.nm_fruta
  RIGHT OUTER JOIN tipo_frutas t ON t.tp_fruta = l.tp_fruta
 GROUP BY t.tp_fruta
 ORDER BY 2 desc
;

-- https://prestodb.io/docs/current/functions/window.html
SELECT l.nm_fruta
     , sum(v.qtd_venda) total_vendas
     , rank() OVER (ORDER BY sum(v.qtd_venda) DESC) ranking
  FROM csvms.lista_frutas l
  LEFT OUTER JOIN csvms.venda_frutas v ON v.nm_fruta = l.nm_fruta
 GROUP BY l.nm_fruta
;
-- Pivotando a tabela
--reduce_agg(inputValue T, initialState S, inputFunction(S, T, S), combineFunction(S, S, S)) → S
SELECT reduce(agg['amargo'], 0.0, (s, x) -> s + x, s -> s) AS "amargo"
     , reduce(agg['azedo'], 0.0, (s, x) -> s + x, s -> s) AS "azedo"
     , reduce(agg['doce'], 0.0, (s, x) -> s + x, s -> s) AS "doce"
  FROM (
  SELECT multimap_agg(t.tp_fruta, if(v.qtd_venda IS NULL,0,v.qtd_venda) * t.vl_fruta) agg
  FROM venda_frutas v
  RIGHT OUTER JOIN lista_frutas l ON v.nm_fruta = l.nm_fruta
  RIGHT OUTER JOIN tipo_frutas t ON t.tp_fruta = l.tp_fruta) vendas
;

-- https://prestodb.io/docs/0.217/functions/datetime.html
-- Quantas frutas foram vendidas em cada dia
SELECT dt AS "data da vendda"
     , if(agg['banana'] IS NULL,0,
          reduce(agg['banana'], 0.0, (s, x) -> s + x, s -> s)) AS "banana"
     , if(agg['bergamota'] IS NULL,0,
          reduce(agg['bergamota'], 0.0, (s, x) -> s + x, s -> s)) AS "bergamota"
     , if(agg['limão'] IS NULL,0,
          reduce(agg['limão'], 0.0, (s, x) -> s + x, s -> s)) AS "limão"
     , if(agg['maçã'] IS NULL,0,
          reduce(agg['maçã'], 0.0, (s, x) -> s + x, s -> s)) AS "maçã"
  FROM (
  SELECT DATE(v.op_ts) dt
       , multimap_agg(l.nm_fruta, if(v.qtd_venda IS NULL,0,v.qtd_venda)) agg
  FROM (SELECT raw_venda_frutas.op_ts
             , raw_venda_frutas.qtd_venda
             , raw_venda_frutas.nm_fruta
          FROM (SELECT max(op_ts) last
                     , cod_venda
                 FROM raw_venda_frutas
                GROUP BY cod_venda) current
          JOIN raw_venda_frutas 
            ON current.cod_venda = raw_venda_frutas.cod_venda
          AND current.last = raw_venda_frutas.op_ts) v
        RIGHT OUTER JOIN csvms.lista_frutas l ON v.nm_fruta = l.nm_fruta
  GROUP BY DATE(v.op_ts))
  WHERE dt is not NULL
  ORDER BY 1
;

-- EXPLAIN
EXPLAIN ANALYZE VERBOSE  SELECT nm_fruta FROM raw_lista_frutas WHERE tp_fruta = 'doce';
-- https://prestodb.io/docs/current/optimizer.html
-- https://github.com/prestodb/presto/tree/master/presto-spi/src/main/java/com/facebook/presto/spi/plan

--https://randomuser.me/api/?format=json&nat=br&results=5
--aws s3 sync bootcamp/sql/users s3://compass.uol.bootcamp/users
CREATE EXTERNAL TABLE csvms.randomuser (
  results array<
    struct<
      `gender`:string,
      `name`:struct<
        `title`:string,
        `first`:string,
        `last`:string>,
      `location`:struct<
        `street`:struct<
            `number`:int,
            `name`:string>,
        `city`:string,
        `state`:string,
        `country`:string,
        `postcode`:int,
        `coordinates`:struct<
            `latitude`:float,
            `longitude`:float>,
        `timezone`:struct<
            `offset`:string,
            `description`:string>>,
      `email`:string,
      `login`:struct<
          `uuid`:string,
          `username`:string,
          `password`:string,
          `salt`:string,
          `md5`:string,
          `sha1`:string,
          `sha256`:string>,
      `dob`:struct<
          `date`:timestamp,
          `age`:string>,
      `registered`:struct<
          `date`:timestamp,
          `age`:string>,
      `phone`:string,
      `cell`:string,
      `picture`:struct<
          `large`:string,
          `medium`:string,
          `thumbnail`:string>,
      `nat`:string
    >
  >,
  info struct<
    `seed`:string,
    `results`:int,
    `page`:int,
    `version`:string
  >)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://compass.uol.bootcamp/users'
;

SELECT randomuser.info.seed
     , randomuser.info.results
     , randomuser.info.page
     , randomuser.info.version
  FROM csvms.randomuser
;

CREATE OR REPLACE VIEW users as
SELECT users.results.gender
     , users.results.name.title
     , users.results.name.first
     , users.results.name.last
     , users.results.location.street.number
     , users.results.location.street.name
     , users.results.location.city
     , users.results.location.state
     , users.results.location.country
     , users.results.location.postcode
     , users.results.location.coordinates.latitude
     , users.results.location.coordinates.longitude
     , users.results.location.timezone.offset
     , users.results.location.timezone.description
     , users.results.email
     , users.results.login.uuid
     , users.results.login.username
     , users.results.login.password
     , users.results.login.salt
     , users.results.login.md5
     , users.results.login.sha1
     , users.results.login.sha256
     , users.results.dob.date as dob
     , users.results.registered.date as registered
     , users.results.phone
     , users.results.cell
     , users.results.picture.large
     , users.results.picture.medium
     , users.results.picture.thumbnail
     , users.results.nat
 FROM csvms.randomuser
CROSS JOIN UNNEST(randomuser.results) AS users (results)
;

--aws s3 sync bootcamp/sql/locals s3://compass.uol.bootcamp/locals
CREATE EXTERNAL TABLE csvms.raw_locations (
    results STRING)
STORED AS TEXTFILE
LOCATION 's3://compass.uol.bootcamp/locals';

-- https://prestodb.io/docs/0.217/functions/json.html
CREATE OR REPLACE VIEW csvms.locations AS
  WITH prep AS (
SELECT json_extract_scalar(results, '$') results
  FROM csvms.raw_locations)
SELECT CAST(json_extract(prep.results,'$.city') AS VARCHAR) city
     , CAST(json_extract(prep.results,'$.state') AS VARCHAR) state
     , CAST(json_extract(prep.results,'$.country') AS VARCHAR) country
     , regexp_extract_all(
        CAST(json_extract(prep.results,'$.coordinates') AS VARCHAR) 
      , '[0-9.-]+') coordinates
  FROM prep
;
