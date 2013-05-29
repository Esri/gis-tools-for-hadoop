package com.esri.hadoop.examples.trip;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TripCorrRed extends Reducer<Text, TripCorrWrit, Text, Text> {

	// Start with the destination cells for one origin cell
	// Output the percentage for the most common destination cell (and maybe some attributes)
	public void reduce(Text key, Iterable<TripCorrWrit> values, Context ctx)
		throws IOException, InterruptedException {

		final int INIT_SIZE = 8000;	 // how densely clumped in downtown Tokyo?
		HashMap<String,Long> records = new HashMap<String,Long>(INIT_SIZE);

		String sval, maxDest = null;
		long totCount = 0, maxCount = 0;
		for (TripCorrWrit entry : values) {
			sval = entry.toString();  // bounds of destination cell - value of iterator, key of hashmap
			long newCount = records.containsKey(sval) ? 1 + records.get(sval) : 1;
			records.put(sval, newCount);
			if (newCount > maxCount) {
				maxCount = newCount;
				maxDest = sval;
			}
			totCount++;
		}  // /for
		Configuration config = ctx.getConfiguration();
		int minPoints = config.getInt("com.esri.trip.threshold", 10);  //minimum count per cell
		minPoints = minPoints < 2 ? 1 : minPoints;
		if (totCount >= minPoints) {
			double pct = 0.;
			if (maxCount > 1)  // if only one trip going to each destination cell, report zero correlation.
				pct = 100. * (double)maxCount / (double)totCount;
			ctx.write(key, new Text(String.format("%d\t%d\t%f\t%s",
												  totCount, maxCount, pct,	// calculated numbers
												  maxDest)));  // most common destination cell (bounds)
		}
	}

}
