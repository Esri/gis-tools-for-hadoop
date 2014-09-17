package com.esri.hadoop.examples.pointinpolygon;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
	@SuppressWarnings("static-access")
	public static int main(String[] init_args) throws Exception {		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();

		Options options = new Options();
		options.addOption(OptionBuilder.withDescription("input HDFS path to CSV points file").hasArg().isRequired().create("csv"));
		options.addOption(OptionBuilder.withDescription("input HDFS path to JSON polygon file").hasArg().isRequired().create("json"));
		options.addOption(OptionBuilder.withDescription("output HDFS path").hasArg().isRequired().create("out"));
		
		GnuParser parser = new GnuParser();
		CommandLine cmd = null;
		
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			printUsage(options);
			System.exit(1);
		}
		
		String inputCSV = cmd.getOptionValue("csv");
		String inputJSON = cmd.getOptionValue("json");
		String output = cmd.getOptionValue("out");
		
		config.set("sample.features.input", inputJSON);
		config.set("sample.features.keyattribute", "NAME");
		config.setInt("samples.csvdata.columns.lat", 1);
		config.setInt("samples.csvdata.columns.long", 2);
		
		Job job = Job.getInstance(config);

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
		
		TextInputFormat.setInputPaths(job, new Path(inputCSV));
		TextOutputFormat.setOutputPath(job, new Path(output));

		job.setJarByClass(AggregationSampleDriver.class);

		return job.waitForCompletion(true)?  0 : 1;
	}
	
	static void printUsage(Options options)
	{
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("bin/run-sample point-in-polygon <args>", options);
	}
}
