package com.esri.hadoop.examples.trip;

public class DegreeMinuteSecondUtility {

	public static double parseDms(String inVal) {  // DMS string to decimal degrees
		try {
			String[] tmp = inVal.split("\\.");
			double outVal = Double.parseDouble(tmp[0])
				+ Double.parseDouble(tmp[1].substring(0,2)) / 60
				+ Double.parseDouble(tmp[1].substring(2)) / 360000 ;
			return outVal;
		} catch (Exception dmsx) {
			// log?  System.out.println("parseDms: " + inVal);
			return 0;
		}
	}

}
