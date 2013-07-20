package com.esri.hadoop.examples.trip;

import java.util.ArrayList;
import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;

public class QuadTreeGrid {
	private double lonMin, lonMax, arcLon, latMin, latMax, latExtent;
	private int xCount, yCount;
	private ArrayList<double[]> gridAsArray;
	private QuadTree quadTree;
	private QuadTreeIterator quadTreeIter;

	QuadTreeGrid(double gridSide, Geometry studyArea) {
		buildGrid(gridSide, studyArea);
	}

	// gridSide: Nominal length of side of grid cell (meters)
	private void buildGrid(double gridSide, Geometry studyArea) {
		Envelope envelope = new Envelope();
		studyArea.queryEnvelope(envelope);
		Envelope2D env2d;
		env2d = new Envelope2D(envelope.getXMin(), envelope.getYMin(),
							   envelope.getXMax(), envelope.getYMax());
		quadTree = new QuadTree(env2d, 8);
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

		int enough = (int)(Math.ceil(.000001 + latExtent*htOneDeg/gridSide)) *
			(int)(Math.ceil(.000001 + (lonMax-lonMin)*lenOneDegBaseline/gridSide));
		gridAsArray = new ArrayList<double[]>(enough);
		double xlon, ylat;
		int flatIndex = 0;
		for (ylat = latMin, yCount = 0;  ylat < latMax;  yCount++) {
			fromPt.setXY(lonMin, ylat);
			toPt.setXY(lonMin+arcLon, ylat);
			double xlen = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
			double height = cellArea/xlen;  // meters
			double arcLat = height / htOneDeg;
			for (xlon = lonMin, xCount = 0;  xlon < lonMax;  xlon += arcLon, xCount++) {
				Polygon rect = new Polygon();
				rect.startPath(xlon, ylat);
				rect.lineTo(xlon+xlen, ylat);
				rect.lineTo(xlon+xlen, ylat+height);
				rect.lineTo(xlon, ylat+height);
				rect.closeAllPaths();
				// Filter out cells more than APPROXIMATELY 2km from country polygon
				// (geodesic length traversed by an angle of arc differs by latitude & orientation).
				// Simple overlap would lose some points with error/offset, and
				// islands/fill missing from the country polygon (as seen on basemap)
				// GeometryEngine.overlaps(rect, studyArea)
				if (GeometryEngine.distance(rect, studyArea, null) < .02) {  // angle in degrees
					double[] tmp = {xlon, ylat, xlon+arcLon, ylat+arcLat};
					gridAsArray.add(tmp);
					quadTree.insert(flatIndex++,
									new Envelope2D(xlon, ylat, xlon+arcLon, ylat+arcLat));
				}
			}  // /for X
			ylat += arcLat;
		}  // /for Y
		quadTreeIter = quadTree.getIterator();
	}  // buildGrid

	
	/**
	 * Return the bounds of the cell identified by cellIndex, as array
	 * 
	 * @param cellIndex
	 * @return bounds of the cell identified by cellIndex [lhs,bot,rhs,top]
	 */
	public double[] cellBounds(int cellIndex) {
		return gridAsArray.get(cellIndex);
	}

	
	/**
	 * Query the grid for the index of the cell containing the given lon-lat
	 * 
	 * @param longitude
	 * @param latitude
	 * @return index to cell in grid, or <0 if not found
	 */
	public int queryGrid(double longitude, double latitude) {
		Point pt = new Point(longitude, latitude);
		// reset iterator to the quadrant envelope that contains the point passed
		quadTreeIter.resetIterator(pt, 0);

		int eltHandle;
		while ((eltHandle = quadTreeIter.next()) >= 0) {
			int flatIndex = quadTree.getElement(eltHandle);
	 		// we know the point and the cell are in the same quadrant, but we need to
	 		// make sure the cell actually contains the point
			double[] cell = gridAsArray.get(flatIndex);
			Envelope envp = new Envelope(cell[0], cell[1], cell[2], cell[3]);
			if (envp.contains(pt)) {  // utilize knowledge that grid is rectangular
				return flatIndex;
			}
		}
		
		// feature not found
		return -1;
	}
}
