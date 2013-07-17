This sample MapReduce application, consists of the source code for
[Vehicle Trip Discovery with GIS Tools for Hadoop](http://blogs.esri.com/esri/arcgis/#tbd).
It finds places within the study area, that have the highest numbers of trips with nearly common
origin and destination locations, using GPS position data.
We defined trips based on a stoppage time of more than 15 minutes between data points.
The application aggregates the inferred trips by the grid cell containing the trip origin position -
using a grid of cells about 500m a side.  For each cell containing one or more trip origins,
it counts trips by grid cell of the destination position, and notes the count of trips to the
most-common destination cell for that origin cell.

The data used during development, is not redistributable.
File `sample-vehicle-positions.csv` contains a few records of arbitrary data that is not expected
to have characteristics of real-life vehicle data, but rather serves to illustrate the CSV format,
which is:

1. vehicle-ID - used to identify records from the same car
2. date in YYMMDD
3. time in HHMMSS
4. longitude in degrees-minutes-seconds (DDD.MMSSSS)
5. latitude in degrees-minutes-seconds (DD.MMSSSS)
6. compass bearing in degrees
7. speed in km/h
8. road type code

Two ways to run the sample are provided:
* Command-line using the Hadoop CLI (Command-Line Interface)
* ArcGIS Geoprocessing Tools

#### Prerequisites

* Recommend running the earthquake aggregation sample (point-in-polygon-aggregation-mr) first before this one
* A clone of this repository
* Access to a Hadoop cluster, or a local distribution of Hadoop
* A Git client that can pull from a GitHub repository
* A Linux/Unix environment is **highly** recommended
* ArcGIS is necessary to run the Geoprocessing tools, or to import and visualize the output

#### Build System

The build system used for this sample is [Apache Ant](http://ant.apache.org/) with [Maven Ant Tasks](http://maven.apache.org/ant-tasks/download.html) for dependency management.  

***

### Command Line ##

Make sure `hadoop` is in the path set in your environment.
Then please adapt the following recipes to your data.

`env HADOOP_CLASSPATH=../lib/esri-geometry-api.jar:../lib/spatial-sdk-hadoop.jar  hadoop  jar trip-discovery.jar com.esri.hadoop.examples.trip.TripCellDrv  -libjars ../lib/esri-geometry-api.jar,../lib/spatial-sdk-hadoop.jar 15 500 sample-study-area.json sample-vehicle-positions.csv out-trip-1`

`env HADOOP_CLASSPATH=../lib/esri-geometry-api.jar hadoop jar trip-discovery.jar com.esri.hadoop.examples.trip.TripCorrDrv -libjars ../lib/esri-geometry-api.jar 2 'out-trip-1/part-r-*' out-trip-2`

### Geoprocessing tools ###

See `./gp/README.md` for instructions on how to run the sample using the Geoprocessing Tools for Hadoop.
