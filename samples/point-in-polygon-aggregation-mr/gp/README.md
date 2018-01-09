### Running the GP Model sample

#### Setting up the model for ArcGIS Geoprocessing


1. Download and set up the [Geoprocessing Tools from Hadoop](https://github.com/Esri/geoprocessing-tools-for-hadoop)
2. Create a directory to put the GP model and sample data in, such as `C:\esri-gp-sample`
3. Clone or download the source of this repository
4. Extract `gis-tools-for-hadoop/samples/data/samples.gdb.zip` to your local sample directory
5. Copy the files from `gis-tools-for-hadoop/samples/data/point-in-polygon-aggregation-mr/gp` to the same directory
6. Now open ArcCatalog or ArcMap and connect to the sample directory
7. At this point, you should be able to open the model `RunSampleApplication` in the `SampleModel` toolbox

> If you have red X's on the GP Tools from the Hadoop Tools toolbox, you will need to double click rectangle in the model and reset the location of the tool.

#### Setting up the data in Hadoop

The only data file you need to put on HDFS is `samples/data/earthquakes.csv`.  The other files are required for the oozie worfklow job to run correctly.

Your directory structure in HDFS should look something like this:

```
/user/ 
  name/
    gp-sample/
      data/
        earthquakes.csv
      job/
        aggregation-sample.jar
        esri-geometry-api-2.0.0.jar
        spatial-sdk-json-2.0.0.jar
        workflow.xml
```

* `esri-geometry-api-2.0.0.jar` and `spatial-sdk-json-2.0.0.jar` can be found in `samples/lib`.  
* `aggregation-sample.jar` is in the root directory of this sample
* `workflow.xml` is in this directory

Once you have the files set up in HDFS, you will need to update your `job.properties` file to match.  Assuming the stucture provided above, this is what the properties file should look like:

```
nameNode=hdfs://localhost:8020
jobTracker=localhost:8021
baseDir=${nameNode}/user/name/gp-sample
inputDir=${baseDir}/data/earthquakes.csv
outputDir=${baseDir}/output
sampleFeatures=${baseDir}/polygons.json
oozie.wf.application.path=${baseDir}/job
oozie.libpath=${oozie.wf.application.path}
queueName=default
user.name=hdfs
```
