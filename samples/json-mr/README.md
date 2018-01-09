This JSON-MR mini-sample demonstrates the use of JSON data as the MapReduce input.
It works with both Esri GeoServices REST JSON and GeoJSON, in both Enclosed and Unenclosed forms.

By contrast, the point-in-polygon-aggregation-mr sample demonstrates the possibly more common case of
delimited text for the MapReduce input, and side-loading JSON reference or study-area data, of moderate size,
in the setup method of the Mapper class.

The JSON-MR sample application accepts command-line arguments to specify Esri or GeoJSON,
in Enclosed or Unenclosed form - examples follow:

```
ant  # may need to adjust Hadoop version in build.xml
hadoop fs -put data/*.json data/
export HADOOP_CLASSPATH=../lib/esri-geometry-api-2.0.0.jar:../lib/spatial-sdk-json-2.0.0.jar
libjars="-libjars ../lib/esri-geometry-api-2.0.0.jar,../lib/spatial-sdk-json-2.0.0.jar"

# hdfs dfs -rmdir eejs-out >/dev/null 2>&1 || /bin/true
hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${libjars} enc esri data/test15eej.json eejs-out

# hdfs dfs -rmdir egjs-out >/dev/null 2>&1
hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${libjars} enc geojs data/test15egj.json egjs-out

# hdfs dfs -rmdir uejs-out >/dev/null 2>&1
hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${libjars} unenc esri data/test15uej.json uejs-out

# hdfs dfs -rmdir ugjs-out >/dev/null 2>&1
hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${libjars} unenc geojs data/test15ugj.json ugjs-out
```
