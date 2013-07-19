package com.esri.hadoop.examples.trip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TripCellDriver {

	/**
	 * Infer trips, with origin & destination cells
	 */
	public static void main(String[] init_args) throws Exception {
		Configuration config = new Configuration();

		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String[] args = new GenericOptionsParser(config, init_args).getRemainingArgs();

		/*
		 * Command-line parameters
		 *  [0] threshold stopping time to delineate trips, in minutes (default 15 min)
		 *  [1] grid cell size (nominal or target length of side of equal-area cell, in meters or km)
		 *  [2] path to Esri JSON file of Japan country polygon
		 *  [3] path(s) to the input data source
		 *  [4] path to write the output of the MapReduce jobs
		 */
		if (args.length != 5) {
			System.out.println("Invalid Arguments");
			print_usage();
			throw new IllegalArgumentException();
		}

		config.set("com.esri.trip.threshold", args[0]);
		config.set("com.esri.trip.cellsize", args[1]);
		config.set("com.esri.trip.input", args[2]);

		Job job = new Job(config);
		job.setJobName("Automobile Trip Origin & Destination by Grid Cell");
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CarSortWritable.class);
		job.setOutputValueClass(TripCellWritable.class);

		job.setMapperClass(TripCellMapper.class);
		job.setReducerClass(TripCellReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, new Path(args[3]));
		TextOutputFormat.setOutputPath(job, new Path(args[4]));

		job.setJarByClass(TripCellDriver.class);

		System.exit( job.waitForCompletion(true) ? 0 : 1 );
	}

	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar trip-discovery.jar TripCellDrv -libjars [external jar references] tripBreakTime cellSize [/hdfs/path/to]/japan-country.json [/hdfs/path/to]/vehicle-positions.csv [/hdfs/path/to/user]/vehicle-output");
		System.out.println("***");
	}

}
