package com.esri.hadoop.examples.trip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

	/**
	 * For each grid cell containing trip origins, count the trips to a common destination cell
	 */
public class TripInCommonDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TripInCommonDriver(), args));
    }

	public int run(String[] args) throws Exception {
        Configuration config = getConf();

		/*
		 * Command-line parameters
		 *  [0] minimum number of trips starting from origin cell
		 *  [1] path to the input (intermediate) data
		 *  [2] path to write the output of the MapReduce jobs
		 */
		if (args.length != 3) {
			System.out.println("Arguments ~ " + args.length + ": " + args[0] + "|" + args[1]);
			System.out.println("Invalid Arguments");
			print_usage();
			throw new IllegalArgumentException();
		}

		config.set("com.esri.trip.threshold", args[0]);

		Job job = new Job(config);
		job.setJobName("Automobile Trip Origin & Destination by Grid Cell");
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TripInCommonWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TripInCommonMapper.class);
		job.setReducerClass(TripInCommonReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setJarByClass(TripInCommonDriver.class);

		return( job.waitForCompletion(true) ? 0 : 1 );
	}

	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar trip-discovery.jar TripInCommonDriver -libjars [external jar references] minCount [/hdfs/path/to]/trip-cells.csv [/hdfs/path/to/user]/vehicle-output");
		System.out.println("***");
	}

}
