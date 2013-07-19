package com.esri.hadoop.examples.trip;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TripCellWritable implements Writable {

	private Text startDate;
	private Text startTime;
	private Text startLon;
	private Text startLat;
	private Text startSpeed;
	private DoubleWritable startLhs;
	private DoubleWritable startBot;
	private DoubleWritable startRhs;
	private DoubleWritable startTop;
	private Text endDate;
	private Text endTime;
	private Text endLon;
	private Text endLat;
	private Text endSpeed;
	private DoubleWritable endLhs;
	private DoubleWritable endBot;
	private DoubleWritable endRhs;
	private DoubleWritable endTop;

    public TripCellWritable(String startDate, String startTime, String startLon, String startLat, String startSpeed,
							double startLhs, double startBot, double startRhs, double startTop,
							String endDate, String endTime, String endLon, String endLat, String endSpeed,
							double endLhs, double endBot, double endRhs, double endTop) {
	    set(new Text(startDate),
			new Text(startTime),
			new Text(startLon),
			new Text(startLat),
			new Text(startSpeed),
			new DoubleWritable(startLhs),
			new DoubleWritable(startBot),
			new DoubleWritable(startRhs),
			new DoubleWritable(startTop),
			new Text(endDate),
			new Text(endTime),
			new Text(endLon),
			new Text(endLat),
			new Text(endSpeed),
			new DoubleWritable(endLhs),
			new DoubleWritable(endBot),
			new DoubleWritable(endRhs),
			new DoubleWritable(endTop)
			);
	}

	public String getDate1() { return startDate.toString(); }  // start/origin
	public String getTime1() { return startTime.toString(); }
	public String getLon1() { return startLon.toString(); }
	public String getLat1() { return startLat.toString(); }
	public String getSpd1() { return startSpeed.toString(); }
	public double getLhs1() { return startLhs.get(); }
	public double getBot1() { return startBot.get(); }
	public double getRhs1() { return startRhs.get(); }
	public double getTop1() { return startTop.get(); }
	public String getDate2() { return endDate.toString(); }  // end/destination
	public String getTime2() { return endTime.toString(); }
	public String getLon2() { return endLon.toString(); }
	public String getLat2() { return endLat.toString(); }
	public String getSpd2() { return endSpeed.toString(); }
	public double getLhs2() { return endLhs.get(); }
	public double getBot2() { return endBot.get(); }
	public double getRhs2() { return endRhs.get(); }
	public double getTop2() { return endTop.get(); }

	@Override
	public void readFields(DataInput inp) throws IOException {
		startDate.readFields(inp);
		startTime.readFields(inp);
		startLon.readFields(inp);
		startLat.readFields(inp);
		startSpeed.readFields(inp);
		startLhs.readFields(inp);
		startBot.readFields(inp);
		startRhs.readFields(inp);
		startTop.readFields(inp);
		endDate.readFields(inp);
		endTime.readFields(inp);
		endLon.readFields(inp);
		endLat.readFields(inp);
		endSpeed.readFields(inp);
		endLhs.readFields(inp);
		endBot.readFields(inp);
		endRhs.readFields(inp);
		endTop.readFields(inp);
	}

	public void set(Text pStDate, Text pStTime, Text pStLon, Text pStLat, Text pStSpeed,
					DoubleWritable lhs1, DoubleWritable bot1, DoubleWritable rhs1, DoubleWritable top1,
					Text pEndDate, Text pEndTime, Text pEndLon, Text pEndLat, Text pEndSpeed,
					DoubleWritable lhs2, DoubleWritable bot2, DoubleWritable rhs2, DoubleWritable top2) {
		startDate = pStDate;
		startTime = pStTime;
		startLon = pStLon;
		startLat = pStLat;
		startSpeed = pStSpeed;
		startLhs = lhs1;
		startBot = bot1;
		startRhs = rhs1;
		startTop = top1;
		endDate = pEndDate;
		endTime = pEndTime;
		endLon = pEndLon;
		endLat = pEndLat;
		endSpeed = pEndSpeed;
		endLhs = lhs2;
		endBot = bot2;
		endRhs = rhs2;
		endTop = top2;
	}

	/**
	 * This is one record of output of our MapReduce job.
	 */
	@Override
	public String toString()
	{
		return String.format("%s\t%s\t%s\t%s\t%s\t%f\t%f\t%f\t%f\t%s\t%s\t%s\t%s\t%s\t%f\t%f\t%f\t%f",
							 getDate1(), getTime1(), getLon1(), getLat1(), getSpd1(),
							 getLhs1(), getBot1(), getRhs1(), getTop1(),
							 getDate2(), getTime2(), getLon2(), getLat2(), getSpd2(),
							 getLhs2(), getBot2(), getRhs2(), getTop2());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		startDate.write(out);
		startTime.write(out);
		startLon.write(out);
		startLat.write(out);
		startSpeed.write(out);
		startLhs.write(out);
		startBot.write(out);
		startRhs.write(out);
		startTop.write(out);
		endDate.write(out);
		endTime.write(out);
		endLon.write(out);
		endLat.write(out);
		endSpeed.write(out);
		endLhs.write(out);
		endBot.write(out);
		endRhs.write(out);
		endTop.write(out);
	}

}
