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

/**
 * Equal-area hexagon mesh, hexagons of which have vertical edges at left & right
 * (rather than horizontal edges at top & bottom).
 * UNTESTED as of 2013/07/01.
 */
public class HexagonMesh {
	private double lonMin, lonMax, arcSide, arcWidth, latMin, latMax, latExtent;
	private int xCount, yCount;
	private ArrayList<Polygon> meshAsArray;
	private QuadTree quadTree;
	private QuadTreeIterator quadTreeIter;

	HexagonMesh(double gridSide, Geometry studyArea) {
		buildMesh(gridSide, studyArea);
	}

	// gridSide: Length of side of square of same area as grid cell (meters)
	private void buildMesh(double gridSide, Geometry studyArea) {
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
		final double latOneDeg = latMid + 1;
		toPt.setXY(lonMin, latOneDeg);
		final double htOneDeg = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);

		arcWidth = gridSide * Math.sqrt(Math.sqrt(4/3)) / lenOneDegBaseline;  // longitude arc of hexagon width
		double halfWidth = arcWidth/2;

		int enough = (int)(Math.ceil(.31 + latExtent*htOneDeg/gridSide)) *
			(int)(Math.ceil(.54 + (lonMax-lonMin)*lenOneDegBaseline/gridSide));
		meshAsArray = new ArrayList<Polygon>(enough);
		double lhs, ctr, rhs, bot, mid1, mid2, top;
		int flatIndex = 0;
		fromPt.setXY(lonMin, latMin);
		toPt.setXY(lonMin+arcWidth, latMin);
		double widLen = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
		bot = cellArea/(3*widLen);
		for (mid1 = latMin, yCount = 0;  bot < latMax;  mid1 = top, yCount++) {
			fromPt.setXY(lonMin, mid1);
			toPt.setXY(lonMin+arcWidth, mid1);
			widLen = GeometryEngine.geodesicDistanceOnWGS84(fromPt, toPt);
			double sideHtLen = 2 * cellArea / (3 * widLen);  // meters
			double arcHtSide = sideHtLen / htOneDeg;         // degrees
			double roofArea = cellArea - widLen*(sideHtLen + (mid1-bot)*htOneDeg/2);  // m^2
			mid2 = mid1 + arcHtSide;
			top = mid2 + 2*roofArea/widLen;

			//lhs = (yCount % 2 == 0) ? lonMin : lonMin - halfWidth;
			lhs = lonMin - halfWidth * (yCount % 2);
			for (xCount = 0;  lhs < lonMax;  lhs = rhs, xCount++) {
				Polygon hexagon = new Polygon();
				ctr = lhs+halfWidth;
				rhs = lhs+arcWidth;
				hexagon.startPath(ctr, bot);
				hexagon.lineTo(rhs, mid1);
				hexagon.lineTo(rhs, mid2);
				hexagon.lineTo(ctr, top);
				hexagon.lineTo(lhs, mid2);
				hexagon.lineTo(lhs, mid1);
				hexagon.closeAllPaths();
				// Filter out cells more than APPROXIMATELY 2km from country polygon
				// (geodesic length traversed by an angle of arc differs by latitude & orientation).
				// Simple overlap would lose some points with error/offset, and
				// islands/fill missing from the country polygon (as seen on basemap)
				// GeometryEngine.overlaps(hexagon, studyArea)
				if (GeometryEngine.distance(hexagon, studyArea, null) < .02) {  // angle in degrees
					meshAsArray.add(hexagon);
					quadTree.insert(flatIndex++, new Envelope2D(lhs, bot, rhs, top));
				}
			}  // /for X

			//bot = (yCount % 2 == 0) ? mid1 + arcHtSide : mid1;
			bot = mid1 + arcHtSide;
		}  // /for Y
		quadTreeIter = quadTree.getIterator();
	}  // buildMesh

	
	/**
	 * Return the cell identified by cellIndex, as Polygon
	 * 
	 * @param cellIndex
	 * @return polygon of the cell identified by cellIndex
	 */
	public Polygon cellBounds(int cellIndex) {
		return meshAsArray.get(cellIndex);
	}

	
	/**
	 * Return the cell identified by cellIndex, as WKT
	 * 
	 * @param cellIndex
	 * @return WKT of the cell identified by cellIndex
	 */
	// public String cellAsText(int cellIndex) {
	// 	Polygon hexagon = meshAsArray.get(cellIndex);
	// 	return GeometryEngine.geometryToWkt(hexagon,
	// 										WktExportFlags.wktExportPolygon);
	// }
	// public String cellAsTsv(int cellIndex) {
	// 	return String.format("%s\t%s\t%s\t%s\t%s\t%s", // ctr = (lhs+rhs)/2
	// 						 bot, rhs, mid1, mid2, top, lhs);
	// }
// 	public String cellAsString(int cellIndex) {
// 		return cellAsText(cellIndex);
// 	}

	
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
			Polygon cell = meshAsArray.get(flatIndex);
			if (GeometryEngine.contains(cell, pt, null /*spatialReference*/)) {
				return flatIndex;
			}
		}
		
		// feature not found
		return -1;
	}
}
