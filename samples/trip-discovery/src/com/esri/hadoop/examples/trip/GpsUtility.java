package com.esri.hadoop.examples.trip;

public class GpsUtility {

	/**
	 * Degree-Minutes-Seconds string to decimal degrees
	 * 
	 * @param inVal [D]DD.MMSSSS
	 * @return angle in degrees as double
	 */
	public static double parseDms(String inVal) {
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

	/**
	 * Convert date & time as strings to interval data number
	 * 
	 * @param sDate YYMMDD
	 * @param sTime HHMMSS
	 * @return Long integer representing relative datetime.
	 */
	public static long timeAsInteger(String sDate, String sTime) {
		long nTime = 3600 * Long.parseLong(sTime.substring(0,2))  + 
			60 * Long.parseLong(sTime.substring(2,4))  +
			Long.parseLong(sTime.substring(4));  // seconds after midnight
		if (sDate != null) {
			nTime += 86400 * Long.parseLong(sDate);
		}
		return nTime;
	}

}
