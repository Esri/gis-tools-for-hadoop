package com.esri.hadoop.examples.trip;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TripInCommonMapper extends Mapper<LongWritable, Text, Text, TripInCommonWritable> {

	// column indices for values in the intermediate origin-destination TSV
	static final int COL_CAR  = 0;   // car ID
	static final int COL_DAT1 = 1;   // date in YYMMDD - ORIGIN
	static final int COL_TIM1 = 2;   // time in HHMMSS
	static final int COL_LON1 = 3;   // longitude in DMS
	static final int COL_LAT1 = 4;   // latitude in DMS
	static final int COL_SPD1 = 5;   // speed in km/h
	static final int COL_LHS1 = 6;   // left of origin cell - decimal degrees
	static final int COL_BOT1 = 7;   // bottom of origin cell decimal degrees
	static final int COL_RHS1 = 8;   // right of origin cell decimal degrees
	static final int COL_TOP1 = 9;   // top of origin cell - decimal degrees
	static final int COL_DAT2 = 10;  // date in YYMMDD - DESTINATION
	static final int COL_TIM2 = 11;  // time in HHMMSS
	static final int COL_LON2 = 12;  // longitude in DMS
	static final int COL_LAT2 = 13;  // latitude in DMS
	static final int COL_SPD2 = 14;  // speed in km/h
	static final int COL_LHS2 = 15;  // left of dest cell - decimal degrees
	static final int COL_BOT2 = 16;  // bottom of dest cell decimal degrees
	static final int COL_RHS2 = 17;  // right of dest cell decimal degrees
	static final int COL_TOP2 = 18;  // top of dest cell - decimal degrees

	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		/* 
		 * The TextInputFormat we set in the configuration, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// Note: no header row in this TSV

		String line = val.toString();
		String[] values = line.split("\t");  // no tab inside quoted string in input
		Text key2 = new Text(String.format("%s\t%s\t%s\t%s", values[COL_LHS1], values[COL_BOT1],
										   values[COL_RHS1], values[COL_TOP1]));
		TripInCommonWritable data = new TripInCommonWritable(values[COL_LHS2], values[COL_BOT2],
															 values[COL_RHS2], values[COL_TOP2]);
		context.write(key2, data);

	}

}
