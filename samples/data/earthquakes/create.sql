CREATE EXTERNAL TABLE earthquakes (earthquake_date STRING, latitude DOUBLE, longitude DOUBLE, depth double, magnitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${table.location}';