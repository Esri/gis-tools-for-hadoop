package com.esri.hadoop.examples.trip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.esri.json.EsriFeatureClass;


// Note: we do not consider overnight trips.
//  Could be done with carID only as the key, and
//  CarDateTime-Writable with compareTo on car-date-time (larger sort)
public class TripCellReducer extends Reducer<Text, CarSortWritable, Text, TripCellWritable> {

	private EsriFeatureClass country;
	private int threshold;  // stop-time threshold in seconds
	private EqualAreaGrid grid;
	
	
	/**
	 * Sets up reducer with country geometry provided as argument[0] to the jar
	 */
	@Override
	public void setup(Context context)
	{
		// first pull values from the configuration		
		Configuration config = context.getConfiguration();

		int minutes = config.getInt("com.esri.trip.threshold", 15);  //minutes stoppage delineating trips
		threshold = minutes * 60;  // minutes -> seconds

		double gridSide = 1000.;   // Nominal/average/target length of side of grid cell (meters)
		String sizeArg = config.get("com.esri.trip.cellsize", "1000");
		if (sizeArg.length() > 0 && sizeArg.charAt(0) != '-') {
			double trySize = Double.parseDouble(sizeArg);
			if (trySize >= 100)  //likely unrealistic smaller than about 200m to 500m
				gridSide = trySize;  // input as meters
			else if (trySize > 0)
				gridSide = 1000 * trySize;  // input as km
		}

		String featuresPath = config.get("com.esri.trip.input");
		FSDataInputStream iStream = null;

		try {
			// load the JSON file provided as argument
			FileSystem hdfs = FileSystem.get(config);
			iStream = hdfs.open(new Path(featuresPath));
			country = EsriFeatureClass.fromJson(iStream);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (iStream != null) {
				try {
					iStream.close();
				} catch (IOException e) { }
			}
		}

		// build the grid of cells
		if (country != null) {
			grid = new EqualAreaGrid(gridSide, country.features[0].geometry);
		}
	}


	// Start with the multiple position-data records for one car on one day.
	// Output the discovered trips, as location & cell corners & time for both of origin & destination.
	public void reduce(Text key, Iterable<CarSortWritable> values, Context ctx)
		throws IOException, InterruptedException {

		String[] kys = key.toString().split(",");  // carID & date
		Text outKy = new Text(kys[0]);

		// Expect at most tens of thousands of positions per car per day - expect up to thousands.
		// (per year, up to 2-3 hundreds of thousands)
		final int MAX_BUFFER_SIZE = 8000;  // would fit a record every 11s all day
		ArrayList<CarSortWritable> records = new ArrayList<CarSortWritable>(MAX_BUFFER_SIZE);

		for (CarSortWritable entry : values) {
			records.add(new CarSortWritable(entry));
		}
		Collections.sort(records);

		// Keep origin & last/previous time & position
		CarSortWritable first = records.get(0);
		String theDate = first.getDate(), origTime = first.getTime(),
			origLon = first.getLon(), origLat = first.getLat(), origSpd = first.getSpeed(),
			prevTime = null, prevLon = null, prevLat = null, prevSpd = null;
		long nOrgTm = GpsUtility.timeAsInteger(theDate, origTime), nPrvTm = -1;
		try {				// Check if lapse exceeding threshold.
			// The check for time lapse, without checking position movement,
			// utilizes the fact that these GPS units transmit data only
			// when the car is on - or at least do not transmit data when
			// the key is altogether out of the ignition.
			for (CarSortWritable entry : records) {
				String curTime = entry.getTime(), curLon = entry.getLon(), curLat = entry.getLat(),
					curSpd = entry.getSpeed();
				long nCurTm = GpsUtility.timeAsInteger(theDate, curTime);

				if (nPrvTm > nOrgTm   //ignore lone points
					&& nCurTm > nPrvTm + threshold) {

					int idxOrig = grid.queryGrid(GpsUtility.parseDms(origLon),
												 GpsUtility.parseDms(origLat));
					int idxDest = grid.queryGrid(GpsUtility.parseDms(prevLon),
												 GpsUtility.parseDms(prevLat));
					if (idxOrig >= 0 && idxDest > 0) {  // discard outliers
						double[] cellOrig = grid.cellBounds(idxOrig);
						double[] cellDest = grid.cellBounds(idxDest);
						ctx.write(outKy,
								  new TripCellWritable(theDate, origTime, origLon, origLat, origSpd,
													   cellOrig[0], cellOrig[1], cellOrig[2], cellOrig[3],
													   theDate, prevTime, prevLon, prevLat, prevSpd,
													   cellDest[0], cellDest[1], cellDest[2], cellDest[3])
								  );
					}
					nOrgTm   = nCurTm;
					origTime = curTime;
					origLon  = curLon;
					origLat  = curLat;
					origSpd  = curSpd;
				}
				nPrvTm   = nCurTm;
				prevTime = curTime;
				prevLon  = curLon;
				prevLat  = curLat;
				prevSpd  = curSpd;
			}
			if (/*records.size() > 1 && */ nPrvTm > nOrgTm) {  // no lone point
				int idxOrig = grid.queryGrid(GpsUtility.parseDms(origLon),
											 GpsUtility.parseDms(origLat));
				int idxDest = grid.queryGrid(GpsUtility.parseDms(prevLon),
											 GpsUtility.parseDms(prevLat));
				if (idxOrig >= 0 && idxDest > 0) {  // discard outliers
					double[] cellOrig = grid.cellBounds(idxOrig);
					double[] cellDest = grid.cellBounds(idxDest);
					ctx.write(outKy,
							  new TripCellWritable(theDate, origTime, origLon, origLat, origSpd,
												   cellOrig[0], cellOrig[1], cellOrig[2], cellOrig[3],
												   theDate, prevTime, prevLon, prevLat, prevSpd,  // current, after loop exit
												   cellDest[0], cellDest[1], cellDest[2], cellDest[3]));
				}
			}
		} catch (Exception e) {
			// could log something
		}
	}

}
