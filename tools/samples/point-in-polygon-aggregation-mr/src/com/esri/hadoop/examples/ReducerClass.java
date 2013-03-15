package com.esri.hadoop.examples;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerClass extends Reducer<Text, EarthquakeDataWritable, Text, EarthquakeDataWritable> {
	
	public void reduce(Text key, Iterable<EarthquakeDataWritable> values, Context ctx) throws IOException, InterruptedException{
		
		int agg_cnt = 0;
		float agg_avgMag = 0;
		float agg_minMag = 999;
		float agg_maxMag = 0;
		
		/* What we've got here is a key (county name such as 'Riverside') and a
		 * list of every earthquake that occurred inside that county's boundary.
		 * All we need to do is loop and aggregate the data in the list.
		 */
		for (EarthquakeDataWritable data : values)
		{
			int cnt = data.getCnt();
			float avgMag = data.getAvg();
			float minMag = data.getMin();
			float maxMag = data.getMax();
			
			agg_cnt += cnt;
			agg_avgMag = (agg_avgMag == 0) ? avgMag : (agg_avgMag + avgMag) / 2;
			
			agg_minMag = Math.min(agg_minMag, minMag);
			agg_maxMag = Math.max(agg_maxMag, maxMag);
		}
		

		EarthquakeDataWritable array = new EarthquakeDataWritable(agg_cnt, agg_avgMag, agg_minMag, agg_maxMag);
		
		
		ctx.write(key, array);
	}
	
}
