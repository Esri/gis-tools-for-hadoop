This sample tool aggregates earthquake data for counties in California using a custom MapReduce job.  Two ways to run the sample are provided :

* Command-line using the Hadoop CLI (Command-Line Interface)
* ArcGIS Geoprocessing Tools

> The automated scripts and models provided attempt to remove some of the complexity that is inherent in Hadoop, but there will be some initial configuration required to make them work with your system.

#### Prerequisites

* Have a clone of this repository
* Access to a Hadoop cluster, or a local distribution of Hadoop
* A Git client that can pull from a GitHub repository
* A Unix environment is **highly** recommended for command line usage
* ArcGIS is necessary to run the GP tools

#### Build System

The build system used for this sample is [Apache Ant](http://ant.apache.org/) with [Maven Ant Tasks](http://maven.apache.org/ant-tasks/download.html) for dependency management.  

***

### Command Line ##

1. Make sure `hadoop` is set in your path environment.
2. Move to `point-in-polygon-aggregation-mr/cmd`.
3. Edit the sample configuration script `sample-config.sh` to point to your Hadoop cluster.
4. Run the basic sample script `run-sample-basic.sh`

`sample-config.sh`
```bash
NAME_NODE_URL=hdfs://localhost:8020
JOB_TRACKER_URL=localhost:8021

# this is the directory in HDFS where all the sample files will go
SAMPLE_DIR=/user/name/samples/point-in-polygon
```

> The job driver `main()` takes three arguments.  The driver class is only used when running the job
> directly from the command line (and not using Oozie)
>  1. Path to Esri JSON file (counties.json)  
>  2. Path to Input file (earthquakes.csv)  
>  3. Output Path

> The **run-sample** scripts are pretty straight forward and can be opened to see how they work.

***

### Geoprocessing tools ###

Move the directory `./gp` for instructions on how to run the sample using the geoprocessing tools.
