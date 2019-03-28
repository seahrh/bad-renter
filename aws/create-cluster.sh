#!/bin/bash

aws emr --profile seahrh create-cluster --name "spark_cluster" \
--release-label emr-5.22.0 \
--applications Name=Spark Name=Hive Name=Hue Name=Tez Name=HCatalog Name=Ganglia \
--configurations https://s3-ap-southeast-1.amazonaws.com/com.sgcharts.ap-southeast-1/emr/configurations.json \
--ec2-attributes KeyName=emr \
--instance-type r5.xlarge --instance-count 1 \
--use-default-roles
