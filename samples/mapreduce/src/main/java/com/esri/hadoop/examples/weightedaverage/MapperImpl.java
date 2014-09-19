package com.esri.hadoop.examples.weightedaverage;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.hadoop.examples.QuadTreeHelper;
import com.esri.json.EsriFeature;
import com.esri.json.EsriFeatureClass;


public class MapperImpl extends Mapper<LongWritable, Text, Text, WeightedAverageWritable> {
	
	// column indices for values in the CSV
	int longitudeIndex;
	int latitudeIndex;
	int averageIndex;
	boolean csvHasHeader;
	
	// in ca_counties.json, the label for the polygon is "NAME"
	String labelAttribute;
	
	QuadTreeHelper<EsriFeature> index;

	// it's usually a good idea to reuse objects used inside the map()
	// method to cut down on excess object creation/garbage collection.
	Point point = new Point();
	Text outKey = new Text();
	WeightedAverageWritable outValue = new WeightedAverageWritable();
	
	/**
	 * Sets up mapper with filter geometry provided as argument[0] to the jar
	 */
	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();
		
		// first pull values from the configuration		
		String featuresPath = config.get(ToolConstants.FEATURE_INPUT_PATH);
		labelAttribute = config.get(ToolConstants.FEATURE_KEY_ATTRIBUTE, "NAME");
		latitudeIndex = config.getInt(ToolConstants.CSV_INDEX_LATITUDE, 1);
		longitudeIndex = config.getInt(ToolConstants.CSV_INDEX_LONGITUDE, 2);
		averageIndex = config.getInt(ToolConstants.CSV_INDEX_FIELD_TO_AVERAGE, 4);
		csvHasHeader = config.getBoolean(ToolConstants.CSV_HAS_HEADER, false);
		
		FSDataInputStream iStream = null;
		
		EsriFeatureClass featureClass = null;
		
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
			IOUtils.closeQuietly(iStream);
		}
		
		// calculate full extent of all features
		Envelope2D fullExtent = new Envelope2D();
		Envelope2D featureExtent = new Envelope2D();
		for (EsriFeature feature : featureClass.features) {
			feature.geometry.queryLooseEnvelope2D(featureExtent);
			fullExtent.merge(featureExtent);
		}
		
		index = new QuadTreeHelper<EsriFeature>(fullExtent, 8);
		index.setSpatialReference(SpatialReference.create(4326));
		
		// iterate features and insert them into the quadtree
		if (featureClass != null){
			for (EsriFeature feature : featureClass.features) {
				index.insert(feature.geometry, feature);
			}
		}
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		
		/* 
		 * The TextInputFormat we set in the configuration, by default, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// skip the first line of the CSV if it is a header
		if (csvHasHeader && key.get() == 0) return;
		
		String line = val.toString();
		String [] values = line.split(",");
		
		float latitude, longitude, average;
		
		try {
			latitude = Float.parseFloat(values[latitudeIndex]);
			longitude = Float.parseFloat(values[longitudeIndex]);
			average = Float.parseFloat(values[averageIndex]);
		} catch (NumberFormatException e) {
			// one of these values isn't a valid number, we
			// can't continue with this record
			return;
		}
		
		// set point directly from longitude and latitude
		point.setXY(longitude, latitude);
		
		// Each map only processes one earthquake record at a time, so we start out with our count 
		// as 1.  Aggregation will occur in the combine/reduce stages
		outValue.reset(1, average);
		
		// query index for first feature that contains our point
		EsriFeature feature = index.queryFirst(point, OperatorContains.local(), false);

		if (feature != null){
			String name = (String)feature.attributes.get(labelAttribute);
			
			if (name == null) 
				name = "???";
			
			outKey.set(name);
		} else {
			outKey.set("*Outside Feature Set");
		}
		
		context.write(outKey, outValue);
	}
}
