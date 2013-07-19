package com.esri.hadoop.examples.trip;

import java.util.ArrayList;
import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;

public class EqualAreaGrid {
	private double lonMin, lonMax, arcLon, latMin, latMax, latExtent;
	private int xCount, yCount;
	private ArrayList<Double> rowBots;

	EqualAreaGrid(double gridSide, Geometry studyArea) {
		buildGrid(gridSide, studyArea);
	}

	// gridSide: Nominal length of side of grid cell (meters)
	private void buildGrid(double gridSide, Geometry studyArea) {
		Envelope envelope = new Envelope();
		studyArea.queryEnvelope(envelope);
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
		xCount = (int)(Math.ceil((lonMax-lonMin)/arcLon));

		final double latOneDeg = latMid + 1;
		toPt.setXY(lonMin, latOneDeg);
		final double htOneDeg = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
		int enough = (int)(Math.ceil(1.00001 + latExtent*htOneDeg/gridSide));
		rowBots = new ArrayList<Double>(enough);
		double ylat, arcLat = -99.;

		for (ylat = latMin, yCount = 0;  ylat < latMax;  yCount++) {
			fromPt.setXY(lonMin, ylat);
			toPt.setXY(lonMin+arcLon, ylat);
			double xlen = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
			double height = cellArea/xlen;      // meters
			arcLat = height / htOneDeg;         // degrees
			rowBots.add(ylat);
			ylat += arcLat;
		}
		rowBots.add(ylat + arcLat); // rowBots[yCount] is top, not a bottom
	}

	
	/**
	 * Query the grid for the cell containing the given lon-lat
	 * 
	 * @param cellIndex
	 * @return bounds of the cell identified by cellIndex [lhs,bot,rhs,top]
	 */
	public double[] cellBounds(int cellIndex) {
		int xIdx = cellIndex % xCount;
		int yIdx = cellIndex / xCount;
		double lhs = lonMin + xIdx * arcLon;
		double[] tmp = {lhs,  rowBots.get(yIdx),
						lhs + arcLon,  rowBots.get(1+yIdx)};
		return tmp;
	}

	
	/**
	 * Query the grid for the cell containing the given lon-lat
	 * 
	 * @param longitude
	 * @param latitude
	 * @return bounds of the cell identified by cellIndex [lhs,bot,rhs,top]
	 */
	public double[] cellBounds(double longitude, double latitude) {
		return cellBounds(queryGrid(longitude, latitude));
	}
	
	/**
	 * Query the grid for the cell containing the given lon-lat
	 * 
	 * @param longitude
	 * @param latitude
	 * @return index to cell in grid, or <0 if not found
	 */
	public int queryGrid(double longitude, double latitude) {
		int cellIndex; // xIdx + xCount * yIdx
		if (longitude >= lonMin && longitude <= lonMax  &&
			latitude >= latMin  && latitude <= latMax)  {   // avoid outliers
			int xIdx = queryCol(longitude);
			if (xIdx >= 0 && xIdx < xCount) {
				int yIdx = queryRow(latitude);
				cellIndex = xIdx + xCount * yIdx;
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
	 * Query the grid for the cell containing the given lon-lat
	 * 
	 * @param longitude
	 * @param latitude
	 * @return vertical index to row in grid, or <0 if not found
	 */
	public int queryRow(double latitude) {
		int yIdx = (int)(yCount*(latitude-latMin)/latExtent);  // approximate, to refine
		yIdx = yIdx < yCount ? yIdx : yCount - 1;
		// Expect either correct, or one of either too high or too low, not both
		while (rowBots.get(yIdx) > latitude) {   // bottom too high
			yIdx--;
		}
		while (yIdx < yCount && rowBots.get(1+yIdx) < latitude) {   // top too low
			yIdx++;
		}
		return yIdx;
	}

	/**
	 * Query the grid for the cell containing the given lon-lat
	 * 
	 * @param longitude
	 * @param latitude
	 * @return vertical index to row in grid, or <0 if not found
	 */
	public int queryCol(double longitude) {
		int xIdx = (int)((longitude-lonMin)/arcLon);
		return xIdx;
	}
}
