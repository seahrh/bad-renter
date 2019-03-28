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
LOCATION 's3://com.sgcharts.ap-southeast-1/tmp/clean'
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
  `payment_date_week` int comment 'week number of year',
  `payment_date_day` int comment 'day of month',
  `payment_amount` int,
  `rent_amount` int,
  `default_amount` int
)
PARTITIONED BY (ds string comment 'datetime string in YYYYMMDDHH format')
CLUSTERED BY(id) SORTED BY(payment_date) INTO 16 BUCKETS
stored as parquet
;
