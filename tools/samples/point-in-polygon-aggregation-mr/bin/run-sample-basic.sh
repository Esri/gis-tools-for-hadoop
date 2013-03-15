#!/bin/bash

source ./sample-config.sh

hadoop fs -rm -r $SAMPLE_DIR

echo "* setting up sample in $SAMPLE_DIR"
hadoop fs -mkdir $SAMPLE_DIR $DATA_DIR

echo "* copying sample data to HDFS"
hadoop fs -put ../../sample-data/ca_counties.json ../../sample-data/earthquakes.csv $DATA_DIR

echo "* executing MapReduce job"
hadoop jar ../aggregation-sample.jar \
           com.esri.hadoop.examples.AggregationSampleDriver \
           -libjars ../../lib/esri-geometry-api.jar,../../lib/hadoop-utilities.jar \
           hdfs://$DATA_DIR/ca_counties.json \
           hdfs://$DATA_DIR/earthquakes.csv \
           hdfs://$OUTPUT_DIR


rm results.txt

echo "* pulling down results"
hadoop fs -getmerge $OUTPUT_DIR results.txt

echo "* print results"
cat results.txt
