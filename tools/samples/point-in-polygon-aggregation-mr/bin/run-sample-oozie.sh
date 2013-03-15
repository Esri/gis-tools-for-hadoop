#!/bin/bash

source ./sample-config.sh

hadoop fs -rm -r $SAMPLE_DIR

echo "* setting up sample in $SAMPLE_DIR"
hadoop fs -mkdir $SAMPLE_DIR $JOB_DIR $LIB_DIR $DATA_DIR/counties-data $DATA_DIR/earthquake-data

echo "* copying sample data to HDFS"
hadoop fs -put ../../data/counties-data/* $DATA_DIR/counties-data
hadoop fs -put ../../data/earthquake-data/* $DATA_DIR/earthquake-data

echo "* copying java libraries to HDFS"
hadoop fs -put ../../lib/esri-geometry-api.jar ../../lib/hadoop-utilities.jar $LIB_DIR

echo "* copying oozie application to HDFS"
hadoop fs -put ../aggregation-sample.jar $JOB_DIR
hadoop fs -put ../workflow.xml $JOB_DIR

# build temp.job.properties based on parameters in sample-config.sh
echo "* creating temp.job.properties"
echo "nameNode=$NAME_NODE_URL" > temp.job.properties
echo "jobTracker=$JOB_TRACKER_URL" >> temp.job.properties
echo "oozie.wf.application.path=$NAME_NODE_URL$JOB_DIR" >> temp.job.properties
echo "oozie.libpath=$NAME_NODE_URL$LIB_DIR" >> temp.job.properties
echo "inputDir=$NAME_NODE_URL$DATA_DIR/counties-data/earthquakes.csv" >> temp.job.properties
echo "esriGeometry=$NAME_NODE_URL$DATA_DIR/earthquake-data/california-counties.json" >> temp.job.properties
echo "outputDir=$NAME_NODE_URL$OUTPUT_DIR" >> temp.job.properties
echo "queueName=default" >> temp.job.properties
echo "user.name=hdfs" >> temp.job.properties

echo "* running oozie job"
oozie job -config temp.job.properties -run
