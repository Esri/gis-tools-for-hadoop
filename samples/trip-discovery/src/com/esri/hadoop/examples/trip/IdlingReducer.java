package com.esri.hadoop.examples.trip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;

public class IdlingReducer extends Reducer<Text, CarSortWritable, Text, TripCellWritable> {
	SpatialReference spatialReference;
	int threshold;  // stop-time threshold in seconds

	@Override
	public void setup(Context context)
	{
		Configuration config = context.getConfiguration();
		int minutes = config.getInt("com.esri.trip.threshold", 15);  //minutes stoppage delineating trips
		threshold = minutes * 60;  // minutes -> seconds
	}

	// Start with the multiple position-data records for one car on one day.
	// Output info on GPS data transmitted when the car seems to be stopped.
    //  In trip discovery, we had used the fact that cars turn off - this one looks
	//  for indication of how much rely on such heuristic.
	// Idea: how much of the idling, reflects cars in a service garage?
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
		//  Idea: medoid vs origin
		CarSortWritable first = records.get(0);  // idea: poll()
		String theDate = first.getDate(), origTime = first.getTime(), startTime = origTime,
			startLon = first.getLon(), startLat = first.getLat(), startSpd = first.getSpeed();
		//prevTime = null, prevLon = null, prevLat = null, prevSpd = null
		// originTime <= idleStartTime <= previousTime <= currentTime
		long nOrgTm = GpsUtility.timeAsInteger(theDate, startTime),
			nStrTm = nOrgTm, nPrvTm = -1, nCurTm = -1;
		double distMoved = -1, roamDist = 100;
		Point fromPt = new Point(GpsUtility.parseDms(startLon),GpsUtility.parseDms(startLat)), toPt = new Point(0,0);
		boolean hasMoved = false, already = false;
		//int pointCount = 0; // no, a non-zero point count can happen when nearly stopped at end of prev trip.
		try {
			for (CarSortWritable entry : records) {
				// Check if lapse exceeding threshold, etc.
				String curTime = entry.getTime(), curLon = entry.getLon(), curLat = entry.getLat(),
					curSpd = entry.getSpeed();
				nCurTm = GpsUtility.timeAsInteger(theDate, curTime);
				double dLon = GpsUtility.parseDms(curLon), dLat = GpsUtility.parseDms(curLat);
				// System.out.println(String.format("%s:%s=%d|%s=%d|%s=%d", key.toString(),
				// 								 startTime, nOrgTm,
				// 								 prevTime, nPrvTm, curTime, nCurTm));
				// Idea: may need to check if the car idles (same position, speed 0)
				//       for the duration of the threshold time (subjective interpretation)
				if (nPrvTm > nOrgTm) {   //ignore lone points
					if (nCurTm > nPrvTm + threshold) {  // infer trip
						// idea: the TripCellReducer could check if the car in fact has moved more than roaming dist.
						nOrgTm = nStrTm = nCurTm;
						origTime = startTime = curTime;
						already = hasMoved = false;
					} else {  // look for subjective false negatives of trips: idling, key in but motor off
						// geodesicDistanceOnWGS84 is approximation as we are using a different GCS, but expect it
						// to be a good approximation as we are using distance only, not positions, with it.
						if (!hasMoved) {  // check if just now it moved past roaming threshold
							toPt.setXY(dLon, dLat);
							distMoved = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
							hasMoved = distMoved > roamDist;  // meters
						}
						if (hasMoved) {  // not idle, restart observations
							nStrTm   = nCurTm;
							startTime = curTime;
							startLon  = curLon;
							startLat  = curLat;
							startSpd  = curSpd;
							fromPt.setXY(dLon, dLat);
							already = hasMoved = false;
						} else {  // In this one, we want to output data when the car did not move
							if (nCurTm > nStrTm + threshold  // substantive idling, not stoplight
								&& !already) {  // output once per idle sequence
								long elapsed = nCurTm-nStrTm;
								ctx.write(outKy,
										  new TripCellWritable(theDate, startTime, startLon, startLat, startSpd,
															   elapsed, elapsed/60., fromPt.getX(), fromPt.getY(),
															   theDate, curTime, curLon, curLat, curSpd,
															   distMoved, 0, toPt.getX(), toPt.getY())
										  );
								already = true;
							}
						}
					}
				}
				nPrvTm   = nCurTm;
				//prevTime = curTime;
				//prevLon  = curLon;
				//prevLat  = curLat;
				//prevSpd  = curSpd;
			}
		} catch (Exception e) {
			// could log something
		}
	}

}
