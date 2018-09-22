package com.griddynamics;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimestampWritableComparable  implements WritableComparable<TimestampWritableComparable> {
	private long beginTimestamp;
	private long endTimestamp;
	private long sessionDuration;

	public TimestampWritableComparable(){
		this.beginTimestamp=0L;
		this.endTimestamp=0L;
		this.sessionDuration=0L;
	}

	public TimestampWritableComparable(long bt, long et){
		this.beginTimestamp=bt;
		this.endTimestamp= et;
		this.sessionDuration = et-bt;
	}

	public long getBeginTimestamp() {
		return beginTimestamp;
	}

	public void setBeginTimestamp(long beginTimestamp) {
		this.beginTimestamp = beginTimestamp;
	}

	public long getEndTimestamp() {
		return endTimestamp;
	}

	public void setEndTimestamp(long endTimestamp) {
		this.endTimestamp = endTimestamp;
	}

	public long getSessionDuration() {
		return sessionDuration;
	}

	public void setSessionDuration(long sessionDuration) {
		this.sessionDuration = sessionDuration;
	}

	@Override
	public int compareTo(TimestampWritableComparable that) {
		long thisValue = this.sessionDuration;
		long thatValue = that.sessionDuration;
		return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(beginTimestamp);
		dataOutput.writeLong(endTimestamp);
		dataOutput.writeLong(sessionDuration);

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		beginTimestamp = dataInput.readLong();
		endTimestamp = dataInput.readLong();
		sessionDuration = dataInput.readLong();
	}


	@Override
	public String toString() {
		return beginTimestamp + "," + endTimestamp+ "," +sessionDuration;
	}

}
