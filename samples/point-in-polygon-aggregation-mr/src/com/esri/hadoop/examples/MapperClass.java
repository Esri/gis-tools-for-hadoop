package com.esri.hadoop.examples;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeature;
import com.esri.json.EsriFeatureClass;


public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// column indices for values in the CSV
	int longitudeIndex;
	int latitudeIndex;
	

	// in ca_counties.json, the label for the polygon is "NAME"
	String labelAttribute;
	
	EsriFeatureClass featureClass;
	SpatialReference spatialReference;
	
	/**
	 * Sets up mapper with filter geometry provided as argument[0] to the jar
	 */
	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();
		
		spatialReference = SpatialReference.create(4326);

		// first pull values from the configuration		
		String featuresPath = config.get("sample.features.input");
		labelAttribute = config.get("sample.features.keyattribute", "NAME");
		latitudeIndex = config.getInt("samples.csvdata.columns.lat", 1);
		longitudeIndex = config.getInt("samples.csvdata.columns.long", 2);
		
		
		
		FSDataInputStream iStream = null;
		
		spatialReference = SpatialReference.create(4326);
		
		
		try {
			// load the JSON file provided as argument 0
			FileSystem hdfs = FileSystem.get(config);
			iStream = hdfs.open(new Path(featuresPath));
			featureClass = EsriFeatureClass.fromJson(iStream);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			throw();
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
	}
	
	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {
		
		/* 
		 * The TextInputFormat we set in the configuration, by default, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// We know that the first line of the CSV is just headers, so at byte offset 0 we can just return
		if (key.get() == 0) return;
		
		
		String line = val.toString();
		String [] values = line.split(",");
		
		// Note: We know the data coming in is clean, but in practice it's best not to
		//       assume clean data.  This is especially true with big data processing
		float latitude = Float.parseFloat(values[latitudeIndex]);
		float longitude = Float.parseFloat(values[longitudeIndex]);
		
		// Create our Geometry directly from longitude and latitude
		Geometry point = new Point(longitude, latitude);
		
		boolean found = false;

		// Each map only processes one earthquake record at a time, so we start out with our count 
		// as 1.  Aggregation will occur in the combine/reduce stages
		IntWritable one = new IntWritable(1);
		
		
		// Loop through every feature in our feature class
		for (EsriFeature feature : featureClass.features)
		{
			if (GeometryEngine.contains(feature.geometry, point, spatialReference))
			{
				String name = (String)feature.attributes.get(labelAttribute);
				if (name == null) name = "???";
				
				context.write(new Text(name), one);
				
				found = true;
				break;
			}
		}
		
		if (!found)
		{
			context.write(new Text("*Outside Feature Set"), one);
		}
	}
}
