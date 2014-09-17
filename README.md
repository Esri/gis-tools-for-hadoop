gis-tools-for-hadoop
====================

The [__GIS Tools for Hadoop__](http://esri.github.io/gis-tools-for-hadoop/) are a collection of GIS tools that leverage 
the [Spatial Framework for Hadoop](https://github.com/Esri/spatial-framework-for-hadoop)
for spatial analysis of big data.  The tools make use of 
the [Geoprocessing Tools for Hadoop](https://github.com/Esri/geoprocessing-tools-for-hadoop) toolbox,
to provide access to the Hadoop system from the ArcGIS Geoprocessing environment. 

## Features

* Sample tools that demonstrate full stack implementations of all the resources provided to solve GIS problems 
using Hadoop


**Resources for building custom tools**
* [Spatial Framework for Hadoop](https://github.com/Esri/spatial-framework-for-hadoop) 
 * Java helper utilities for Hadoop developers
 * Hive spatial user-defined functions 
* [Esri Geometry API Java](https://github.com/Esri/geometry-api-java) - Java geometry library for spatial data 
processing 
* [Geoprocessing Tools](https://github.com/Esri/geoprocessing-tools-for-hadoop) - ArcGIS Geoprocessing tools 
for Hadoop


## Getting Started

You will need access to a machine with Hadoop and Hive set up.  Visit [Apache](http://wiki.apache.org/hadoop/GettingStartedWithHadoop), [Hortonworks](http://hortonworks.com/get-started), or [Cloudera](http://www.cloudera.com) for more information on getting started with Hadoop.

1. **Build the samples**

   Use [Apache Maven](http://maven.apache.org) to build the project.

   ```
   mvn clean package
   ```
   
2. **Load the sample data into HDFS**

   ```
   ./bin/load-sample-data --hdfs-path gis_samples --with-hive --hive-database gis_samples
   ```

   You should now be able to see the sample data in HDFS:

   ```bash
   $ hdfs dfs -ls -R gis_samples
   drwxr-xr-x   - mike hdfs          0 2014-09-17 14:02 gis_samples/counties
   -rw-r--r--   3 mike hdfs    1028330 2014-09-17 14:02 gis_samples/counties/california-counties.json
   drwxr-xr-x   - mike hdfs          0 2014-09-17 14:02 gis_samples/earthquakes
   -rw-r--r--   3 mike hdfs    5742716 2014-09-17 14:02 gis_samples/earthquakes/earthquakes.csv
   [...]
   ```

   and the tables in Hive:
   
   ```bash
   $ hive -S -e "use gis_samples;show tables;"
   [...]
   counties
   earthquakes
   [...]
   ````
   
3. **Run a sample mapreduce job**

   ```
   ./bin/run-sample point-in-polygon -csv gis_samples/earthquakes/ -json gis_samples/counties/california-counties.json -out gis_samples/output
   ```
   
   Print the results:
   
   ```bash
   $ hdfs dfs -cat gis_samples/output/*
   *Outside Feature Set    76817
   Fresno  11
   Imperial        28
   Inyo    20
   Kern    36
   Los Angeles     18
   [...]
   ```
   
4. **Run a sample Hive query**

   Start Hive.  You may need to add the gis-tools assembly to your Hadoop classpath before starting Hive. 
   
   ```bash
   export HADOOP_CLASSPATH=assembly/target/gis-tools-hadoop-assembly-2.0.jar
   hive -S
   ```
   
   Add the gis-tools assembly to your Hive session and create a few temporary functions used by the query.  A full list of functions can be found [here](https://github.com/Esri/spatial-framework-for-hadoop/blob/master/hive/function-ddl.sql)
   
   ```sql
   add jar assembly/target/gis-tools-hadoop-assembly-2.0.jar;
   create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
   create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
   use gis_samples;
   ```
   
   Run the query.
   
   ```sql
   SELECT counties.name, count(*) cnt FROM counties
   JOIN earthquakes
   WHERE ST_Contains(counties.boundaryshape, ST_Point(earthquakes.longitude, earthquakes.latitude))
   GROUP BY counties.name
   ORDER BY cnt desc;
   ```

## Requirements

Requirements will differ depending on the needs of each tool or template. At a minimum, you will need:

* Access to an [Apache Hadoop](http://hadoop.apache.org) cluster
* [ArcGIS for Desktop](http://www.esri.com/software/arcgis/arcgis-for-desktop) or 
[Server](http://www.esri.com/software/arcgis/arcgisserver) for geoprocessing and visualization

Other requirements may include:

* [Apache Hive](http://hive.apache.org/) in order to run Hive queries
* [Apache Oozie Workflow Scheduler](http://oozie.apache.org/) for workflow scheduling

Additional requirements will be spelled out by tools or templates.

## Resources

* [ArcGIS Geodata Resource Center]( http://resources.arcgis.com/en/communities/geodata/)
* [ArcGIS Blog](http://blogs.esri.com/esri/arcgis/)
* [twitter@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing)

## Licensing
Copyright 2013 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's 
[license.txt]( https://raw.github.com/Esri/gis-tools-for-hadoop/master/license.txt) file.

[](Esri Tags: ArcGIS, GIS, Analysis, Big Data, GP, Geoprocessing, Hadoop, Hive, Oozie, Workflow, JSON, Java)
[](Esri Language: Python)

