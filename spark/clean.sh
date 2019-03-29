#!/bin/bash

spark-submit --master yarn \
--deploy-mode cluster \
--class com.sgcharts.badrenter.Cleaner \
s3://com.sgcharts.ap-southeast-1/deploy/bad-renter-assembly-0.1.0.jar \
\
--src_db rent \
--src_table raw \
--sink_db rent \
--sink_table payment \
--sink_partition ds='20190326' \
--sink_path s3://com.sgcharts.ap-southeast-1/hive/warehouse/rent.db/payment/ds=20190326
--default_dob 19800101 \
