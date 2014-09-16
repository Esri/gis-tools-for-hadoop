package com.esri.hadoop.examples.tripdiscovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeatureClass;


// Note: we do not consider overnight trips.
//  Could be done with carID only as the key, and
//  CarDateTime-Writable with compareTo on car-date-time (larger sort)
public class TripCellReducer extends
		Reducer<Text, CarSortWritable, Text, TripCellWritable> {

	double lonMin, lonMax, arcLon, latMin, latMax, latExtent;
	int xCount, yCount;
	Envelope envelope;
	EsriFeatureClass country;
	ArrayList<double[]> grid;
	SpatialReference spatialReference;
	int threshold;  // stop-time threshold in seconds

	private void buildGrid(double gridSide) {   // Nominal length of side of grid cell (meters)
		double cellArea = gridSide*gridSide;
		latMax = envelope.getYMax() + .005;  // +.005 to catch nearby outliers
		latMin = envelope.getYMin() - .005;  // -.005 to catch nearby outliers
		final double latMid = (latMax + latMin) / 2;  // idea: latitude of half area (ctry or envp?)
		latExtent = latMax-latMin;
		lonMin = envelope.getXMin() - .005;  // -.005 to catch nearby outliers (approx. 500m)
		lonMax = envelope.getXMax() + .005;  // +.005 to catch nearby outliers
		final double lonOneDeg = lonMin + 1;  // arbitrary longitude for establishing lenOneDegBaseline
		Point fromPt = new Point(lonMin, latMid),
			toPt = new Point(lonOneDeg, latMid);
		// geodesicDistanceOnWGS84 is an approximation as we are using a different GCS, but expect it
		// to be a good approximation as we are using proportions only, not positions, with it.
		final double lenOneDegBaseline = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
		// GeometryEngine.distance "Calculates the 2D planar distance between two geometries."
		//angle// final double lenOneDegBaseline = GeometryEngine.distance(fromPt, toPt, spatialReference);
		arcLon = gridSide / lenOneDegBaseline;  // longitude arc of grid cell
		final double latOneDeg = latMid + 1;
		toPt.setXY(lonMin, latOneDeg);
		final double htOneDeg = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);

		int enough = (int)(Math.ceil(.000001 + (lonMax-lonMin)*lenOneDegBaseline/gridSide)) *
			(int)(Math.ceil(.000001 + latExtent*htOneDeg/gridSide));
		grid = new ArrayList<double[]>(enough);
		double xlon, ylat;
		// If using quadtree, could filter out cells that do not overlap country polygon
		for (ylat = latMin, yCount = 0;  ylat < latMax;  yCount++) {
			fromPt.setXY(lonMin, ylat);
			toPt.setXY(lonMin+arcLon, ylat);
			double xlen = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
			double height = cellArea/xlen;  // meters
			double arcLat = height / htOneDeg;
			for (xlon = lonMin, xCount = 0;  xlon < lonMax;  xlon += arcLon, xCount++) {
				double[] tmp = {xlon, ylat, xlon+arcLon, ylat+arcLat};
				grid.add(tmp);
			}
			ylat += arcLat;
		}
	}

	
	/**
	 * Query the grid for the cell containing the given lon-lat
	 * 
	 * @param longitude
	 * @param latitude
	 * @return index to cell in array, or <0 if not found
	 */
	private int queryGrid(double longitude, double latitude) {
		int cellIndex; // xIdx + xCount * yIdx
		if (longitude >= lonMin && longitude <= lonMax  &&
			latitude >= latMin  && latitude <= latMax)  {   // avoid outliers
			int xIdx = (int)((longitude-lonMin)/arcLon);
			if (xIdx >= 0 && xIdx < xCount) {
				int yIdx = (int)(yCount*(latitude-latMin)/latExtent);  // approximate, to refine
				yIdx = yIdx < yCount ? yIdx : yCount - 1;
				cellIndex = xIdx + xCount * yIdx;
				// Expect either correct, or one of either too high or too low, not both
				while (grid.get(cellIndex)[1] > latitude) {   // bottom too high
					yIdx--;
					cellIndex -= xCount;
				}
				while (grid.get(cellIndex)[3] < latitude) {   // top too low
					yIdx++;
					cellIndex += xCount;
				}
				if (yIdx < 0 || yIdx >= yCount) {  // bug
					cellIndex = -3;
				}
			} else {  // bug
				cellIndex = -2;
			}
		} else {  // outlier
			cellIndex = -1;
		}
		return cellIndex;
	}
	
	
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
		spatialReference = SpatialReference.create(4301);  //  GCS_Tokyo

		try {
			// load the JSON file provided as argument
			FileSystem hdfs = FileSystem.get(config);
			iStream = hdfs.open(new Path(featuresPath));
			country = EsriFeatureClass.fromJson(iStream);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (iStream != null)
			{
				try {
					iStream.close();
				} catch (IOException e) { }
			}
		}

		// build the grid of cells
		if (country != null) {
			envelope = new Envelope();
			country.features[0].geometry.queryEnvelope(envelope);
			buildGrid(gridSide);
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
		long nOrgTm = timeAsInteger(theDate, origTime), nPrvTm = -1;
		try {				// Check if lapse exceeding threshold.
			// The check for time lapse, without checking position movement,
			// utilizes the fact that these GPS units transmit data only
			// when the car is on - or at least do not transmit data when
			// the key is altogether out of the ignition.
			for (CarSortWritable entry : records) {
				String curTime = entry.getTime(), curLon = entry.getLon(), curLat = entry.getLat(),
					curSpd = entry.getSpeed();
				long nCurTm = timeAsInteger(theDate, curTime);

				if (nPrvTm > nOrgTm   //ignore lone points
					&& nCurTm > nPrvTm + threshold) {

					int idxOrig = queryGrid(DegreeMinuteSecondUtility.parseDms(origLon),
											DegreeMinuteSecondUtility.parseDms(origLat));
					int idxDest = queryGrid(DegreeMinuteSecondUtility.parseDms(prevLon),
											DegreeMinuteSecondUtility.parseDms(prevLat));
					if (idxOrig >= 0 && idxDest > 0) {  // discard outliers
						double[] cellOrig = grid.get(idxOrig);
						double[] cellDest = grid.get(idxDest);
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
				int idxOrig = queryGrid(DegreeMinuteSecondUtility.parseDms(origLon),
										DegreeMinuteSecondUtility.parseDms(origLat));
				int idxDest = queryGrid(DegreeMinuteSecondUtility.parseDms(prevLon),
										DegreeMinuteSecondUtility.parseDms(prevLat));
				if (idxOrig >= 0 && idxDest > 0) {  // discard outliers
					double[] cellOrig = grid.get(idxOrig);
					double[] cellDest = grid.get(idxDest);
					ctx.write(outKy,
							  new TripCellWritable(theDate, origTime, origLon, origLat, origSpd,
												   cellOrig[0], cellOrig[1], cellOrig[2], cellOrig[3],
												   theDate, prevTime, prevLon, prevLat, prevSpd,
												   cellDest[0], cellDest[1], cellDest[2], cellDest[3])); // current, after loop exit
				}
			}
		} catch (Exception e) {
			// could log something
		}
	}

	private long timeAsInteger(String sDate, String sTime) {
		long nTime = 3600 * Long.parseLong(sTime.substring(0,2))  + 
			60 * Long.parseLong(sTime.substring(2,4))  +
			Long.parseLong(sTime.substring(4));  // seconds after midnight
		if (sDate != null) {
			nTime += 86400 * Long.parseLong(sDate);
		}
		return nTime;
	}

}
