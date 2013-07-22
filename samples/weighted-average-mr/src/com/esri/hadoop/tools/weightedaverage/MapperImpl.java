package com.esri.hadoop.tools.weightedaverage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeatureClass;


public class MapperImpl extends Mapper<LongWritable, Text, Text, WeightedAverageWritable> {
	
	// column indices for values in the CSV
	int longitudeIndex;
	int latitudeIndex;
	int averageIndex;
	boolean csvHasHeader;
	
	// in ca_counties.json, the label for the polygon is "NAME"
	String labelAttribute;
	
	EsriFeatureClass featureClass;
	SpatialReference spatialReference;
	QuadTree quadTree;
	QuadTreeIterator quadTreeIter;
	
	private void buildQuadTree(){
		quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), 8);
		
		Envelope envelope = new Envelope();
		for (int i=0;i<featureClass.features.length;i++){
			featureClass.features[i].geometry.queryEnvelope(envelope);
			quadTree.insert(i, new Envelope2D(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
		}
		
		quadTreeIter = quadTree.getIterator();
	}
	
	/**
	 * Query the quadtree for the feature containing the given point
	 * 
	 * @param pt point as longitude, latitude
	 * @return index to feature in featureClass or -1 if not found
	 */
	private int queryQuadTree(Point pt)
	{
		// reset iterator to the quadrant envelope that contains the point passed
		quadTreeIter.resetIterator(pt, 0);
		
		int elmHandle = quadTreeIter.next();
		
		while (elmHandle >= 0){
			int featureIndex = quadTree.getElement(elmHandle);
			
			// we know the point and this feature are in the same quadrant, but we need to make sure the feature
			// actually contains the point
			if (GeometryEngine.contains(featureClass.features[featureIndex].geometry, pt, spatialReference)){
				return featureIndex;
			}
			
			elmHandle = quadTreeIter.next();
		}
		
		// feature not found
		return -1;
	}
	
	
	/**
	 * Sets up mapper with filter geometry provided as argument[0] to the jar
	 */
	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();
		
		spatialReference = SpatialReference.create(4326);

		// first pull values from the configuration		
		String featuresPath = config.get(ToolConstants.FEATURE_INPUT_PATH);
		labelAttribute = config.get(ToolConstants.FEATURE_KEY_ATTRIBUTE, "NAME");
		latitudeIndex = config.getInt(ToolConstants.CSV_INDEX_LATITUDE, 0);
		longitudeIndex = config.getInt(ToolConstants.CSV_INDEX_LONGITUDE, 1);
		averageIndex = config.getInt(ToolConstants.CSV_INDEX_FIELD_TO_AVERAGE, 2);
		csvHasHeader = config.getBoolean(ToolConstants.CSV_HAS_HEADER, false);
		
		FSDataInputStream iStream = null;
		
		try {
			// load the JSON file provided as argument 0
			FileSystem hdfs = FileSystem.get(config);
			iStream = hdfs.open(new Path(featuresPath));
			featureClass = EsriFeatureClass.fromJson(iStream);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		} 
		finally
		{
			if (iStream != null)
			{
				try {
					iStream.close();
				} catch (IOException e) { }
			}
		}
		
		// build a quadtree of our features for fast queries
		if (featureClass != null){
			buildQuadTree();
		}
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		
		/* 
		 * The TextInputFormat we set in the configuration, by default, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// We know that the first line of the CSV is just headers, so at byte offset 0 we can just return
		if (csvHasHeader && key.get() == 0) return;
		
		
		String line = val.toString();
		String [] values = line.split(",");
		
		// Note: We know the data coming in is clean, but in practice it's best not to
		//       assume clean data.  This is especially true with big data processing
		float latitude = Float.parseFloat(values[latitudeIndex]);
		float longitude = Float.parseFloat(values[longitudeIndex]);
		float average = Float.parseFloat(values[averageIndex]);
		
		// Create our Point directly from longitude and latitude
		Point point = new Point(longitude, latitude);
		
		// Each map only processes one earthquake record at a time, so we start out with our count 
		// as 1.  Aggregation will occur in the combine/reduce stages
		WeightedAverageWritable one = new WeightedAverageWritable(1, average);
		
		int featureIndex = queryQuadTree(point);
		
		if (featureIndex >= 0){
			String name = (String)featureClass.features[featureIndex].attributes.get(labelAttribute);
			
			if (name == null) 
				name = "???";
			
			context.write(new Text(name), one);
		} else {
			context.write(new Text("*Outside Feature Set"), one);
		}
	}
}
