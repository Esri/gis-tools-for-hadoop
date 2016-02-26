package com.esri.hadoop.examples.json;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import com.esri.json.hadoop.*;

public class JsonInputSample {

	public static int main(String[] init_args) throws Exception {		
		Configuration config = new Configuration();
		
		// This step is important as init_args contains ALL the arguments passed to hadoop on the command
		// line (such as -libjars [jar files]).  What's left after .getRemainingArgs is just the arguments
		// intended for the MapReduce job
		String [] args = new GenericOptionsParser(config, init_args).getRemainingArgs();
		
		/*
		 * Args
		 *  [0] "ENCLOSED" or "UNENCLOSED"
		 *  [1] "Esri" or "Geo"
		 *  [2] path(s) to the input data source
		 *  [3] path to write the output of the MapReduce jobs
		 */
		if (args.length != 4) {
			System.out.println("Invalid Arguments");
			//print_usage();
			throw new IllegalArgumentException();
		}

		String ARG0 = args[0].toUpperCase();
		String ARG1 = args[1].toUpperCase();
		boolean isEnclosed = "ENCLOSED".equals(ARG0) || "ENC".equals(ARG0) || "E".equals(ARG0);
		boolean isGeoJson = "GEOJSON".equals(ARG1) || "GEOJS".equals(ARG1) || "G".equals(ARG1) ||
			"GJ".equals(ARG1) || "GEO".equals(ARG1);
		
		Job job = new Job(config);

		job.setJarByClass(JsonInputSample.class);
		TextInputFormat.setInputPaths(job, new Path(args[2]));
		job.setMapperClass(RecordsReadMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[3]));

        if (isEnclosed && isGeoJson) {
			job.setJobName("EnclosedGeoJson Sample");
			job.setInputFormatClass(EnclosedGeoJsonInputFormat.class);
		} else if (isGeoJson) {
			job.setJobName("UnenclosedGeoJson Sample");
			job.setInputFormatClass(UnenclosedGeoJsonInputFormat.class);
		} else if (isEnclosed) {
			job.setJobName("EnclosedEsriJson Sample");
			job.setInputFormatClass(EnclosedEsriJsonInputFormat.class);
		} else {
			job.setJobName("UnenclosedEsriJson Sample");
			job.setInputFormatClass(UnenclosedEsriJsonInputFormat.class);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}


	static class RecordsReadMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

			//String line = val.toString();
			//System.err.println(line);  // Shows the record per the JsonRecordReader
			//System.err.println("--");   // clarify distinct records
			IntWritable one = new IntWritable(1);
			context.write(val, one);
		}

	}

}