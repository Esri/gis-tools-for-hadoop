package com.esri.hadoop.examples.trip;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TripInCommonWritable extends ArrayWritable {
	public TripInCommonWritable() { super(Text.class); }
	
    public TripInCommonWritable(String lhs, String bot, String rhs, String top) { //String lon, String lat,
	    super(Text.class, new Writable[] {
				new Text(lhs),
				new Text(bot),
				new Text(rhs),
				new Text(top)
		});
	}

	public String getLhs() { return ((Text)this.get()[0]).toString(); }
	public String getBot() { return ((Text)this.get()[1]).toString(); }
	public String getRhs() { return ((Text)this.get()[2]).toString(); }
	public String getTop() { return ((Text)this.get()[3]).toString(); }

	@Override
	public String toString() {
		return String.format("%s\t%s\t%s\t%s",
							 getLhs(), getBot(), getRhs(), getTop());
	}

}
