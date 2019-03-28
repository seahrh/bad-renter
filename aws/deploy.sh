#!/bin/bash

sbt clean assembly
aws --profile seahrh s3 cp target/scala-2.11/*.jar s3://com.sgcharts.ap-southeast-1/deploy/
