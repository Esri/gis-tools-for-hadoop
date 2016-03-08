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
export HADOOP_CLASSPATH=../lib/esri-geometry-api.jar:../lib/spatial-sdk-hadoop.jar
libjars="-libjars ../lib/esri-geometry-api.jar,../lib/spatial-sdk-hadoop.jar"

hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${lj} enc esri data/test15eej.json eejs-out

hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${lj} enc geojs data/test15egj.json egjs-out

hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${lj} unenc esri data/test15uej.json uejs-out

hadoop jar json-mr-sample.jar com.esri.hadoop.examples.json.JsonInputSample ${lj} unenc geojs data/test15ugj.json ugjs-out
```
