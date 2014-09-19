package com.esri.hadoop.examples.weightedaverage;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ToolDriver
{
	@SuppressWarnings("static-access")
	public static int main(String[] init_args) throws Exception {		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();
		
		Options options = new Options();
		options.addOption(OptionBuilder.withDescription("Input HDFS path to CSV points file").hasArg().isRequired().create("csv"));
		options.addOption(OptionBuilder.withDescription("CSV points longitude column index [default 2 (earthquakes)]").hasArg().withLongOpt("csv-long").create());
		options.addOption(OptionBuilder.withDescription("CSV points latitude column index [default 1 (earthquakes)]").hasArg().withLongOpt("csv-lat").create());
		options.addOption(OptionBuilder.withDescription("CSV points column index of value to average [default 4 (earthquakes)]").hasArg().withLongOpt("csv-avg").create());
		
		options.addOption(OptionBuilder.withDescription("Input HDFS path to JSON polygon file").hasArg().isRequired().create("json"));
		options.addOption(OptionBuilder.withDescription("Use this key when writing the output [default NAME]").hasArg().withLongOpt("json-key").create());
		
		options.addOption(OptionBuilder.withDescription("Output HDFS path").hasArg().isRequired().create("out"));
		
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
		int inputCSVLong = -1;
		int inputCSVLat = -1;
		int inputCSVAverage = -1;
		String inputJSON = cmd.getOptionValue("json");
		String inputJSONKey = cmd.getOptionValue("json-key", "NAME");
		String output = cmd.getOptionValue("out");

		try {
			inputCSVLat = Integer.parseInt(cmd.getOptionValue("csv-lat", "1"));
			inputCSVLong = Integer.parseInt(cmd.getOptionValue("csv-long", "2"));
			inputCSVAverage = Integer.parseInt(cmd.getOptionValue("csv-avg", "4"));
		} catch (NumberFormatException e) {
			System.out.println("Invalid CSV index : " + e.getMessage());
			printUsage(options);
			System.exit(1);
		}
			
		Job job = Job.getInstance(config);
		
		job.setJobName("Weighted Average MapReduce Tool");

		job.getConfiguration().setInt(ToolConstants.CSV_INDEX_FIELD_TO_AVERAGE, inputCSVAverage);
		job.getConfiguration().setInt(ToolConstants.CSV_INDEX_LONGITUDE, inputCSVLong);
		job.getConfiguration().setInt(ToolConstants.CSV_INDEX_LATITUDE, inputCSVLat);
		job.getConfiguration().set(ToolConstants.FEATURE_INPUT_PATH, inputJSON);
		job.getConfiguration().set(ToolConstants.FEATURE_KEY_ATTRIBUTE, inputJSONKey);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WeightedAverageWritable.class);
		
		job.setMapperClass(MapperImpl.class);
		job.setReducerClass(ReducerImpl.class);
		job.setCombinerClass(ReducerImpl.class); // the reducer can act as the combiner in this case
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(inputCSV));
		TextOutputFormat.setOutputPath(job, new Path(output));

		job.setJarByClass(ToolDriver.class);

		return job.waitForCompletion(true)?  0 : 1;
	}
	
	static void printUsage(Options options)
	{
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("bin/run-sample weighted-average <args>", options);
	}
	
	static void print_usage()
	{
		System.out.println("***");
		//System.out.println("Usage: hadoop jar aggregation-sample.jar AggregationSampleDriver -libjars [external jar references] [/hdfs/path/to]/filtergeometry.json [/hdfs/path/to]/earthquakes.csv [/hdfs/path/to/user]/output.out");
		System.out.println("***");
	}
}
