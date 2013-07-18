package com.esri.hadoop.examples.trip;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TripCellMapper extends Mapper<LongWritable, Text, Text, CarSortWritable> {

	// column indices for values in the vehicle CSV
	static final int COL_CAR = 0;  // vehicle ID
	static final int COL_DAT = 1;  // date in YYMMDD
	static final int COL_TIM = 2;  // time in HHMMSS
	static final int COL_LON = 3;  // longitude in DMS
	static final int COL_LAT = 4;  // latitude in DMS
	static final int COL_DIR = 5;  // compass orientation in degrees
	static final int COL_SPD = 6;  // speed in km/h
	static final int COL_ROD = 7;  // road type code

	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		/* 
		 * The TextInputFormat we set in the configuration, splits a text file line by line.
		 * The key is the byte offset to the first character in the line.  The value is the text of the line.
		 */
		
		// Note: no header row in this CSV

		String line = val.toString();
		String[] values = line.split(",");  // no comma in quoted string in input
		Text key2 = new Text(values[COL_CAR] + "," + values[COL_DAT]);
		CarSortWritable data = new CarSortWritable(values[COL_DAT], values[COL_TIM],
												   values[COL_LON], values[COL_LAT],
												   values[COL_DIR], values[COL_SPD], values[COL_ROD]);
		context.write(key2, data);

	}

}
