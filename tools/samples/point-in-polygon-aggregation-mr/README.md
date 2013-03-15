This sample tool aggregates earthquake data for counties in California using a custom MapReduce job.  Three ways to run the sample are provided :

* Command Line (using **Hadoop** directly)
* Command Line (using **Oozie** workflows)
* ArcGIS Geoprocessing Tools

### Prerequisites

* Access to a Hadoop cluster, or a local distribution of Hadoop
* A Git client that can pull from a GitHub repository
* A Unix environment is **highly** recommended for command line usage
* ArcGIS is necessary to run the GP tools

***

### Command Line ##

**Setup**

1. Clone the repository
2. Make sure `hadoop` is set in your path environment.
3. Move to `point-in-polygon-aggregation-mr/bin`.
4. Edit the sample configuration script `sample-config.sh` to point to your Hadoop cluster.


```bash
NAME_NODE_URL=hdfs:/localhost:8020
JOB_TRACKER_URL=localhost:8021

# this is the directory in HDFS where all the sample files will go
SAMPLE_DIR=/user/mike/samples/point-in-polygon
```

**Hadoop**

* Run the basic sample script `run-sample-basic.sh`

**Oozie**

* Run the Oozie sample script `run-sample-oozie.sh`

> The **run-sample** scripts are pretty straight forward and can be opened to see how they work.

***

### Geoprocessing tools ###
**Not implemented yet**
