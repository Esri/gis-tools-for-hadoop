# Aggregation Sample for Hive

First start the Hive Command line (Hive CLI).  If you do not have Hive installed, see [Hive Installation](https://cwiki.apache.org/Hive/adminmanual-installation.html)

```bash
# use '-S' for silent mode
hive -S
```

> This sample assumes that Hive is installed on a local cluster.  If you are using a remote cluster, you will need to move your files to HDFS and change table definitions as needed.

Add the required external libraries and create temporary functions for the geometry api calls.
```bash
add jar
  ${env:HOME}/esri-git/gis-tools-for-hadoop/samples/lib/esri-geometry-api.jar
  ${env:HOME}/esri-git/gis-tools-for-hadoop/samples/lib/spatial-sdk-hadoop.jar
  
create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
```

> This is a minimum implementation the ST_Geometry user definied functions found in the [Hive Spatial Library](https://github.com/ArcGIS/hive-spatial).  The full list of functions is available in the linked repository.

Define a schema for the [earthquake data](https://github.com/Esri/hadoop-tools/tree/master/sample-workflows/data/earthquake-data).  The earthquake data is in CSV (comma-separated values) format, which is natively supported by Hive.

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS earthquakes (earthquake_date STRING, latitude DOUBLE, longitude DOUBLE, magnitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${env:HOME}/esri-git/gis-tools-for-hadoop/samples/data/earthquake-data';
```

Define a schema for the [California counties data](https://github.com/Esri/hadoop-tools/tree/master/sample-workflows/data/counties-data).  The counties data is stored as [Enclosed JSON](https://github.com/Esri/hadoop-tools/wiki/JSON-Formats).  

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS counties (Area string, Perimeter string, State string, County string, Name string, BoundaryShape binary)                                         
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.json.hadoop.serde.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${env:HOME}/esri-git/gis-tools-for-hadoop/samples/data/counties-data'; 
```

Now run a select statement to aggregate earthquake counts accross the California counties.

```sql
SELECT counties.name, count(*) cnt FROM counties
JOIN earthquakes
WHERE ST_Contains(counties.boundaryshape, ST_Point(earthquakes.longitude, earthquakes.latitude))
GROUP BY counties.name
ORDER BY cnt desc;
```

Your results should look like this:

```
Kern  36
San Bernardino	35
Imperial	28
Inyo	20
Los Angeles	18
Riverside	14
Monterey	14
Santa Clara	12
Fresno	11
San Benito	11
San Diego	7
Santa Cruz	5
San Luis Obispo	3
Ventura	3
Orange	2
San Mateo	1
```

===

## run-sample.sql

Alternatively, you can run the entire sample using `run-sample.sql`.

First move to the Hive sample directory and run Hive.

```bash
cd ~/esri-git/gis-tools-for-hadoop/samples/point-in-polygon-aggregation-hive/cmd
hive -S
```

Now run the sample sql file from within Hive

```bash
source run-sample.sql
```
