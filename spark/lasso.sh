#!/bin/bash

spark-submit --master yarn \
--deploy-mode cluster \
--class com.sgcharts.badrenter.LassoRegressionTraining \
s3://com.sgcharts.ap-southeast-1/deploy/bad-renter-assembly-1.0.jar \
\
--src_db rent \
--src_table train \
--partition ds='20190326' \
--sink_db rent \
--sink_table validation \
--sink_path s3://com.sgcharts.ap-southeast-1/hive/warehouse/rent.db/validation \
--model_path s3://com.sgcharts.ap-southeast-1/models/reg_lasso_r2
