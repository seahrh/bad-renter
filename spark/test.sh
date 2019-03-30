#!/bin/bash

spark-submit --master yarn \
--deploy-mode cluster \
--class com.sgcharts.badrenter.ModelTesting \
s3://com.sgcharts.ap-southeast-1/deploy/bad-renter-assembly-0.1.0.jar \
\
--src_db rent \
--src_table payment \
--test_set_first_id 53833 \
--model_path s3://com.sgcharts.ap-southeast-1/models/reg_lasso_r2_