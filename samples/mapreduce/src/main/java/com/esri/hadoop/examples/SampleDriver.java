package com.esri.hadoop.examples;

import org.apache.hadoop.util.ProgramDriver;

public class SampleDriver {

	public static void main(String [] args) throws Throwable {
		ProgramDriver driver = new ProgramDriver();
		
		driver.addClass("point-in-polygon", 
				com.esri.hadoop.examples.pointinpolygon.AggregationSampleDriver.class, 
				"Run point in polygon aggregation sample");
		
		driver.addClass("trip-discovery", 
				com.esri.hadoop.examples.tripdiscovery.TripInCommonDriver.class, 
				"Run trip discovery sample");
		
		driver.addClass("weighted-average", 
				com.esri.hadoop.examples.weightedaverage.ToolDriver.class, 
				"Run weighted average sample");

		driver.run(args);
	}
}
