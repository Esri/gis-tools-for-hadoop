### Running the trip-discovery sample via Geoprocessing in ArcMap

#### Setting up the model for ArcGIS Geoprocessing


1. Download and set up the [Geoprocessing Tools from Hadoop](https://github.com/Esri/geoprocessing-tools-for-hadoop)
2. Create a directory to put the GP model and sample data in, such as `C:\esri-gp-sample`
3. Clone or download the source of this repository
4. Copy the files from `samples/data/trip-discovery/gp` to your local sample directory
5. Now open ArcCatalog or ArcMap
7. Use the Hadoop Tools

> If you have red X's on the GP Tools from the Hadoop Tools toolbox, you will need to double click rectangle in the model and reset the location of the tool.

#### Setting up the data in Hadoop

The only data file you need to put on HDFS is `samples/trip-discovery/sample-vehicle-positions.csv`.  The other files are required for the oozie worfklow to run correctly.

Your directory structure in HDFS should look something like this:

```
/user/ 
  name/
    gp-sample/
      data/
        sample-vehicle-positions.csv
      job/
        esri-geometry-api.jar
        spatial-sdk-hadoop.jar
        trip-discovery.jar
        workflow.xml
```

* `esri-geometry-api.jar` and `spatial-sdk-hadoop` can be found in `samples/lib`.  
* `trip-discovery.jar` is in the root directory of this sample
* `workflow.xml` is in this directory (i.e. this `gp` subdirectory)

Once you have the files set up in HDFS, you will need to update your `job.properties` file to match.  Assuming the stucture provided above, this is what the properties file should look like:

```
nameNode=hdfs://localhost:8020
jobTracker=localhost:8021
baseDir=${nameNode}/user/name/gp-sample
inputDir=${baseDir}/data
outputDir=${baseDir}/output
studyArea=${baseDir}/polygons.json
threshold=15
cellSize=1000
minCommon=1
oozie.wf.application.path=${baseDir}/job
oozie.libpath=${oozie.wf.application.path}
queueName=default
user.name=hdfs
```
