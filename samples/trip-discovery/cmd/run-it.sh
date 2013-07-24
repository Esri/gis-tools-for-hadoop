#!/bin/bash

################ Configure it here ############

#NAME_NODE_URL=hdfs:/localhost:8020
#JOB_TRACKER_URL=localhost:8021
SAMPLE_DIR=/user/randall/trip
STUDY_AREA=sample-study-area.json
DATA_FILE=sample-vehicle-positions.csv
THRESHOLD=15
CELLSIZE=1000
MINCOMMON=1

################ End configuration ############

JOB_DIR=$SAMPLE_DIR/job
LIB_DIR=../../lib
DATA_DIR=$SAMPLE_DIR/data
INTER_DIR=$SAMPLE_DIR/inter
OUTPUT_DIR=$SAMPLE_DIR/output
GEOM_LIB=esri-geometry-api.jar
SPATIAL_SDK=spatial-sdk-hadoop.jar
TRIP_LIB=trip-discovery.jar
RESULTS=results.txt

# Which one works depends on Hadoop version
hadoop fs -rm -r $INTER_DIR $OUTPUT_DIR \
|| hadoop fs -rmr $INTER_DIR $OUTPUT_DIR

echo "* setting up sample in $SAMPLE_DIR"
hadoop fs -mkdir $SAMPLE_DIR $DATA_DIR

echo "* copying sample data to HDFS"
hadoop fs -put ../$STUDY_AREA $DATA_DIR/
hadoop fs -put ../$DATA_FILE $DATA_DIR/

echo "* executing MapReduce jobs"
env HADOOP_CLASSPATH=$LIB_DIR/$GEOM_LIB:$LIB_DIR/$SPATIAL_SDK \
  hadoop jar ../$TRIP_LIB \
           com.esri.hadoop.examples.trip.TripCellDriver \
           -libjars $LIB_DIR/$GEOM_LIB,$LIB_DIR/$SPATIAL_SDK \
           $THRESHOLD  $CELLSIZE  \
           $DATA_DIR/$STUDY_AREA \
           $DATA_DIR/$DATA_FILE \
           $INTER_DIR
env HADOOP_CLASSPATH=$LIB_DIR/$GEOM_LIB:$LIB_DIR/$SPATIAL_SDK \
  hadoop jar ../$TRIP_LIB \
           com.esri.hadoop.examples.trip.TripInCommonDriver \
           -libjars $LIB_DIR/$GEOM_LIB,$LIB_DIR/$SPATIAL_SDK \
           $MINCOMMON \
           $INTER_DIR/p'*' \
           $OUTPUT_DIR

echo "* pulling down results"
rm $RESULTS
hadoop fs -getmerge $OUTPUT_DIR $RESULTS

echo "* print/summarize results"
wc $RESULTS
head -20 $RESULTS
