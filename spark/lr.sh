#!/bin/bash

spark-submit --master yarn \
--deploy-mode cluster \
--class com.sgcharts.badrenter.LinearRegressionTraining \
s3://com.sgcharts.ap-southeast-1/deploy/bad-renter-assembly-0.1.0.jar \
\
--src_db rent \
--src_table payment \
--test_set_first_id 53833 \
