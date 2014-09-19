package com.esri.hadoop.examples.weightedaverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WeightedAverageWritable implements Writable {

	long count;
	double sum;

	public WeightedAverageWritable(){ /* empty constructor is required */ }
	
	public WeightedAverageWritable(long count, double sum){
		this.count = count;
		this.sum = sum;
	}
	
	public void reset(long count, double sum) {
		this.count = count;
		this.sum = sum;
	}

	public long getCount(){
		return count;
	}

	public double getSum(){
		return sum;
	}

	public double getAverage(){
		return sum/count;
	}

	/**
	 * This gets called when the value needs to be deserialized after it has been passed through 
	 * intermediate files
	 */
	@Override
	public void readFields(DataInput input) throws IOException {
		count = input.readLong();
		sum = input.readDouble();
	}

	/**
	 * This gets called when the value is serialized for transport between nodes through intermediate files
	 */
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(count);
		output.writeDouble(sum);
	}

	/**
	 * The toString override gets called when the value is written out to it's final destination file.  This is
	 * when the final average is calculated
	 */
	public String toString(){
		return count + "	" + getAverage();
	}
}
