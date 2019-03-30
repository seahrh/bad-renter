CREATE DATABASE `rent`
LOCATION
  's3://com.sgcharts.ap-southeast-1/hive/warehouse/rent.db'
;

CREATE EXTERNAL TABLE `rent`.`raw`(
  `name` string,`dob` string,`house_id` int,`house_zip` int,`payment_date` string,`payment_amount` int,`rent_amount` int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://com.sgcharts.ap-southeast-1/hive/warehouse/rent.db/raw'
;

CREATE TABLE `rent`.`payment`(
  `id` bigint,
  `name` string,
  `age` int,
  `house_id` int,
  `house_zip` int,
  `payment_date` date,
  `payment_date_year` int,
  `payment_date_month` int,
  `payment_date_day_of_week` int,
  `payment_date_day_of_month` int,
  `payment_amount` int,
  `rent_amount` int,
  `default_amount` int
)
PARTITIONED BY (ds string comment 'datetime string in YYYYMMDD format')
CLUSTERED BY(id) SORTED BY(payment_date) INTO 16 BUCKETS
stored as parquet
;

CREATE TABLE `rent`.`validation`(
  `model` string,
  `result` string,
  `best_params` string
)
stored as parquet
;

CREATE TABLE `rent`.`test`(
  `model` string,
  `r2` double,
  `explained_var` double,
  `mae` double,
  `mse` double,
  `rmse` double
)
stored as parquet
;
