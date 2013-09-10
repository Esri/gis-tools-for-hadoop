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

public class IdlingDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IdlingDriver(), args));
    }

	public int run(String[] args) throws Exception {
        Configuration config = getConf();

		/*
		 * Args
		 *  [0] threshold stopping time to delineate trips (default 15m)
		 *  [1] path to the input data source
		 *  [2] path to write the output of the MapReduce jobs
		 */
		if (args.length != 3)
		{
			System.out.println("Arguments ~ " + args.length + ": " + args[0] + "|" + args[1]);
			System.out.println("Invalid Arguments");
			print_usage();
			throw new IllegalArgumentException();
		}

		config.set("com.esri.trip.threshold", args[0]);

		Job job = new Job(config);
		job.setJobName("Automobile Idling");
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CarSortWritable.class);
		job.setOutputValueClass(TripCellWritable.class);

		job.setMapperClass(TripCellMapper.class);
		job.setReducerClass(IdlingReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setJarByClass(IdlingDriver.class);

		return( job.waitForCompletion(true) ? 0 : 1 );
	}

	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar trip-trip.jar IdlingDriver -libjars [external jar references] 15 [/hdfs/path/to]/trip-positions.csv [/hdfs/path/to/user]/trip-output");
		System.out.println("***");
	}

}
