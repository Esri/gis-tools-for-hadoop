### Running the sample

> This walk through assumes a local single node cluster.  More setup is likely required for a remote cluster. 

This sample aggregates the count of earthquakes, average magnitude and min/max magnitudes, grouped by county. 

The output should look like this (without the column headers):
```
County      #   Avg     Min     Max
Fresno    11	5.38	5.00	6.70
Inyo		20	5.20	5.00	6.10
Kern		36	5.54	5.03	7.50
...
```


Files used by this sample:
```bash
# jar that contains the MapReduce code
sample-workflows/PointInPolygonAggregationSample/bin/aggregation-sample.jar

# california counties that will be use to aggregate the earthquake data
sample-workflows/data/counties-data/california-counties.json

# list of earthquakes since ~1980 with the latitude, longitude and magnitude data for each
sample-workflows/data/earthquake-data/earthquakes.csv

# geometry api for spatial operations (i.e. contains)
sample-workflows/lib/esri-geometry-api.jar

# Hadoop helper utilities for deserializing the Esri JSON format
sample-workflows/lib/hadoop-utilities.jar
```

First, lets make sure Hadoop is set up correctly in the environment

```bash
# note - for older versions of Hadoop, the environment variable is $HADOOP_HOME
echo $HADOOP_PREFIX
/path/to/hadoop-1.0.4
```
If you don't have the environment variable set, your hadoop environment hasn't been set up correctly.

For more information on setting up Hadoop, see [Getting Started With Hadoop](http://wiki.apache.org/hadoop/GettingStartedWithHadoop)

> Some versions of Hadoop have issues with the -libjars options.  If you get an error that says `java.lang.NoClassDefFoundError: com/esri/core/geometry/Geometry`, it might be necessary to add the jars to your hadoop classpath.
```bash
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/esri-git/hadoop-tools/sample-workflows/lib/esri-geometry-api.jar:~/esri-git/hadoop-tools/sample-workflows/lib/hadoop-utilities.jar
```
If you have Hadoop in a remote cluster, you'll need to set the classpath in the clusters configuration.

Run the sample:
```bash
cd ~/esri-git/hadoop-tools/sample-workflows/
${HADOOP_PREFIX}/bin/hadoop jar PointInPolygonAggregationSample/bin/aggregation-sample.jar com.esri.hadoop.examples.AggregationSampleDriver -libjars lib/esri-geometry-api.jar,lib/hadoop-utilities.jar data/counties-data/california-counties.json data/earthquake-data/earthquakes.csv output
```

Print out the results:
```bash
# the output is not actually a single file, but a directory with files with names like 'part-r-00000'
cat output/*
```
