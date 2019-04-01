#!/bin/bash

spark-submit --master yarn \
--deploy-mode cluster \
--class com.sgcharts.badrenter.Sampler \
s3://com.sgcharts.ap-southeast-1/deploy/bad-renter-assembly-0.1.0.jar \
\
--src_db rent \
--src_table payment \
--sink_db rent \
--sink_table train \
--partition ds='20190326' \
--sink_path s3://com.sgcharts.ap-southeast-1/hive/warehouse/rent.db/train/ds=20190326 \
--test_set_first_id 53833 \
--smote_bucket_length 1000 \
--smote_k 10