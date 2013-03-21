#!/bin/bash

if [ -e sample-config.sh ] 
then 
source sample-config.sh
else
echo "ERROR: Could not find configuration file in the current directory.  Are you in the right directory?"
exit 1
fi


hadoop fs -rm -r $SAMPLE_DIR

echo "* setting up sample in $SAMPLE_DIR"
hadoop fs -mkdir $SAMPLE_DIR $DATA_DIR $DATA_DIR/counties-data $DATA_DIR/earthquake-data

echo "* copying sample data to HDFS"
hadoop fs -put ../../data/counties-data/* $DATA_DIR/counties-data
hadoop fs -put ../../data/earthquake-data/* $DATA_DIR/earthquake-data

echo "* executing MapReduce job"
hadoop jar ../aggregation-sample.jar \
           com.esri.hadoop.examples.AggregationSampleDriver \
           -libjars ../../lib/esri-geometry-api.jar,../../lib/hadoop-utilities.jar \
           hdfs://$DATA_DIR/counties-data/california-counties.json \
           hdfs://$DATA_DIR/earthquake-data/earthquakes.csv \
           hdfs://$OUTPUT_DIR


rm results.txt

echo "* pulling down results"
hadoop fs -getmerge $OUTPUT_DIR results.txt

echo "* print results"
cat results.txt
