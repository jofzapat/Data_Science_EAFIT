create external schema myspectrum_schema 
from data catalog 
database 'myspectrum_db' 
iam_role 'arn:aws:iam::471112755118:role/LabRole'
create external database if not exists
;
select count(*) from myspectrum_schema.analisis_riesgo;
;
drop table myspectrum_schema.analisis_riesgo
;
create external table  myspectrum_schema.analisis_riesgo(
tipo varchar(50),
numero integer,
municipio varchar(20),
grupo_diagnostico varchar(71),
nombre_contaminante varchar(50),
sexo varchar(50),
edad varchar(50),
casos integer,
rezago varchar(50),
beta_contaminante varchar(50),
exponencial_beta varchar(50),
limite_inferior_beta varchar(50),
limite_superior_beta varchar(50),
porcentaje_riesgo varchar(50)
)
row format delimited
fields terminated by ';'
stored as textfile
location 's3://jfzapatajlab1/trabajo_1/Analisis_Riesgo/'
;
SELECT *
FROM
    "dev"."myspectrum_schema"."analisis_riesgo"
;

create table Analisis AS
WITH AnalisisData AS (
SELECT * , ROW_NUMBER() OVER () AS row_num
FROM
    "dev"."myspectrum_schema"."analisis_riesgo"
)
SELECT *
FROM AnalisisData
WHERE row_num > 1
;

SELECT *
FROM "dev"."public"."analisis"

;
create table Analisis_etl AS
 SELECT municipio
, grupo_diagnostico
, CASE WHEN sexo = 'Mujer' THEN 'F' WHEN sexo = 'Hombre' THEN 'M' ELSE 'F&M' END AS sexo
, (CASE WHEN  SUBSTRING(edad, 1,  7) = 'Mayores' THEN '> 65' WHEN SUBSTRING(edad, 1, 7) = 'Menores' THEN '< 5'  ELSE '5 - 14' END)  AS edad_años
, CAST( casos as integer) AS casos
, CAST(REPLACE(beta_contaminante, ',', '.') as decimal(5,2))  AS beta_contaminante
, CAST(REPLACE(exponencial_beta , ',', '.') as decimal(5,2))  AS exponencial_beta
, CAST(REPLACE(limite_inferior_beta , ',', '.') as decimal(5,2))  AS limite_inferior_beta
, CAST(REPLACE(limite_superior_beta , ',', '.') as decimal(5,2))  AS limite_superior_beta
, CAST(REPLACE(porcentaje_riesgo , ',', '.') as decimal(5,2))  AS porcentaje_riesgo
FROM
    "dev"."public"."analisis";
