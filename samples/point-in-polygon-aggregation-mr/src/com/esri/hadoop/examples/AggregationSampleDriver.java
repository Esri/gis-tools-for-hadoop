package com.esri.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class AggregationSampleDriver
{
	public static int main(String[] init_args) throws Exception {		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();
		
		/*
		 * Args
		 *  [0] path to Esri JSON file
		 *  [1] path(s) to the input data source
		 *  [2] path to write the output of the MapReduce jobs
		 */
		if (args.length != 3)
		{
			System.out.println("Invalid Arguments");
			print_usage();
			throw new IllegalArgumentException();
		}
		
		config.set("sample.features.input", args[0]);
		config.set("sample.features.keyattribute", "NAME");
		config.setInt("samples.csvdata.columns.lat", 1);
		config.setInt("samples.csvdata.columns.long", 2);
		
		Job job = new Job(config);

		job.setJobName("Earthquake Data Aggregation Sample");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		// In our case, the combiner is the same as the reducer.  This is possible
		// for reducers that are both commutative and associative 
		job.setCombinerClass(ReducerClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setJarByClass(AggregationSampleDriver.class);

		return job.waitForCompletion(true)?  0 : 1;
	}
	
	static void print_usage()
	{
		System.out.println("***");
		System.out.println("Usage: hadoop jar aggregation-sample.jar AggregationSampleDriver -libjars [external jar references] [/hdfs/path/to]/filtergeometry.json [/hdfs/path/to]/earthquakes.csv [/hdfs/path/to/user]/output.out");
		System.out.println("***");
	}
}
