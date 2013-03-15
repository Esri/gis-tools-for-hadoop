package com.esri.hadoop.examples;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

/**
 * 
 * Writable object that stores the aggregate earthquake data.  ArrayWritable is the base class
 * and takes care of (de)serialization for transport between mappers and reducers. 
 */
public class EarthquakeDataWritable extends ArrayWritable {
	public EarthquakeDataWritable() { super(FloatWritable.class); }
	
	public EarthquakeDataWritable(int cnt, float avg, float min, float max)
	{
		super(FloatWritable.class, new Writable[] {
			new FloatWritable(cnt),
			new FloatWritable(avg),
			new FloatWritable(min),
			new FloatWritable(max)
		});
	}
	
	public int getCnt() { return (int)((FloatWritable)this.get()[0]).get(); }
	public float getAvg() { return ((FloatWritable)this.get()[1]).get(); }
	public float getMin() { return ((FloatWritable)this.get()[2]).get(); }
	public float getMax() { return ((FloatWritable)this.get()[3]).get(); }
	
	/**
	 * This is what gets printed out as part of the output of our MapReduce job.  This will be called once for
	 * each county after all the metrics have been aggregated and calculated.
	 */
	@Override
	public String toString()
	{
		return String.format("\t%d\t%.2f\t%.2f\t%.2f", getCnt(), getAvg(), getMin(), getMax());
	}
}