add jar
  ../lib/esri-geometry-api.jar
  ../lib/hadoop-utilities.jar
  ../lib/esri-hive-spatial.jar;

create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';

CREATE EXTERNAL TABLE IF NOT EXISTS earthquakes (earthquake_date STRING, latitude DOUBLE, longitude DOUBLE, magnitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${env:HOME}/esri-git/spatial-tools-hadoop/applications/samples/data/earthquake-data';

CREATE EXTERNAL TABLE IF NOT EXISTS counties (Area string, Perimeter string, State string, County string, Name string, BoundaryShape binary)                                         
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'              
STORED AS INPUTFORMAT 'com.esri.hadoop.hive.serde.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '${env:HOME}/esri-git/spatial-tools-hadoop/applications/samples/data/counties-data';

SELECT counties.name, count(*) cnt FROM counties
JOIN earthquakes
WHERE ST_Contains(counties.boundaryshape, ST_Point(earthquakes.longitude, earthquakes.latitude))
GROUP BY counties.name
ORDER BY cnt desc;

