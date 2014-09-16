package com.esri.hadoop.examples.tripdiscovery;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CarSortWritable extends ArrayWritable implements Comparable<CarSortWritable> {
	public CarSortWritable() { super(Text.class); }

	public CarSortWritable(CarSortWritable that) {
	    super(Text.class, new Writable[] {
				new Text(that.getDate()), new Text(that.getTime()),
				new Text(that.getLon()), new Text(that.getLat()),
				new Text(that.getBearing()), new Text(that.getSpeed()),
				new Text(that.getRoad())
			});
	}
	
    public CarSortWritable(String ymd, String hms, String longitude, String latitude,
						   String orientation, String speed, String roadType) {
	    super(Text.class, new Writable[] {
				new Text(ymd),
				new Text(hms),
				new Text(longitude),
				new Text(latitude),
				new Text(orientation),
				new Text(speed),
				new Text(roadType)
		});
	}

	@Override
	public int compareTo(CarSortWritable that) {
		//String thisDate = this.getDate(), thatDate = that.getDate()
		return
			// (thisDate.equals(that.getDate()) ?
			this.getTime().compareTo(that.getTime())
			// : thisDate.compareTo(thatDate)
			;
	}

	public String getDate() { return ((Text)this.get()[0]).toString(); }
	public String getTime() { return ((Text)this.get()[1]).toString(); }
	public String getLon() { return ((Text)this.get()[2]).toString(); }
	public String getLat() { return ((Text)this.get()[3]).toString(); }
	public String getBearing() { return ((Text)this.get()[4]).toString(); }
	public String getSpeed() { return ((Text)this.get()[5]).toString(); }
	public String getRoad() { return ((Text)this.get()[6]).toString(); }

	/**
	 * This is a record of output of our MapReduce job.
	 */
	@Override
	public String toString()
	{
		return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s", getDate(), getTime(), getLon(), getLat(),
							 getBearing(), getSpeed(), getRoad());
	}

}
