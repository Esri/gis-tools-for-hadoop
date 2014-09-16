package com.esri.hadoop.examples.weightedaverage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerImpl extends Reducer<Text, WeightedAverageWritable, Text, WeightedAverageWritable> {

	// Note : this reducer can also act as a combiner
	public void reduce(Text key, Iterable<WeightedAverageWritable> values, Context ctx) throws IOException, InterruptedException{
		
		long count = 0;
		double sum = 0;
		
		/* What we've got here is a key (county name such as 'Riverside') and a
		 * list of every earthquake that occurred inside that county's boundary.
		 * All we need to do is loop and aggregate the data in the list.
		 */
		for (WeightedAverageWritable val : values)
		{
			count += val.getCount();
			sum += val.getSum();
		}

		ctx.write(key, new WeightedAverageWritable(count, sum));
	}	
}
