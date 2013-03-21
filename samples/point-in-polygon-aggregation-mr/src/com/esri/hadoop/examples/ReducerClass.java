package com.esri.hadoop.examples;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException{
		
		int sumCount = 0;
		
		/* What we've got here is a key (county name such as 'Riverside') and a
		 * list of every earthquake that occurred inside that county's boundary.
		 * All we need to do is loop and aggregate the data in the list.
		 */
		for (IntWritable sum : values)
		{
			sumCount += sum.get();
		}

		ctx.write(key, new IntWritable(sumCount));
	}
	
}
