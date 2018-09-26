package com.griddynamics.customtype;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Class for holding long  values of beginTimestamp,endTimestamp and duration.
 */
public class TimestampWritableComparable  implements WritableComparable<TimestampWritableComparable> {
	private long beginTimestamp;
	private long endTimestamp;
	private long sessionDuration;


	public TimestampWritableComparable(){
		this.beginTimestamp=0L;
		this.endTimestamp= 0L;
		this.sessionDuration = 0L;
	}


	public void setBeginTimestamp(long beginTimestamp) {
		this.beginTimestamp = beginTimestamp;
	}

	public void setEndTimestamp(long endTimestamp) {
		this.endTimestamp = endTimestamp;
	}

	public void setSessionDuration(long sessionDuration) {
		this.sessionDuration = sessionDuration;
	}

	public void setSessionDuration(long beginTimestamp, long endTimestamp) {
		if (!(beginTimestamp<=0 || endTimestamp<=0)) this.sessionDuration = endTimestamp-beginTimestamp;
		else this.sessionDuration=0L;

	}

	public long getBeginTimestamp() {
		return beginTimestamp;
	}

	public long getEndTimestamp() {
		return endTimestamp;
	}

	public long getSessionDuration() {
		return sessionDuration;
	}


	public TimestampWritableComparable(LocalDateTime bt, LocalDateTime et){
		this.beginTimestamp=bt.toEpochSecond(ZoneOffset.UTC);
		this.endTimestamp= et.toEpochSecond(ZoneOffset.UTC);
		if (!et.equals(LocalDateTime.MIN) && !bt.equals(LocalDateTime.MIN)){
			this.sessionDuration = this.beginTimestamp - this.endTimestamp;
		}
	}


	@Override
	public int compareTo(TimestampWritableComparable that) {
		return Long.compare(this.sessionDuration, that.sessionDuration);

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.beginTimestamp);
		dataOutput.writeLong(this.endTimestamp);
		dataOutput.writeLong(this.sessionDuration);

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		beginTimestamp = dataInput.readLong();
		endTimestamp =  dataInput.readLong();
		sessionDuration =  dataInput.readLong();
	}


	@Override
	public String toString() {
		return Long.toString(beginTimestamp)+","+Long.toString(endTimestamp)+","+Long.toString(sessionDuration);
	}


}
