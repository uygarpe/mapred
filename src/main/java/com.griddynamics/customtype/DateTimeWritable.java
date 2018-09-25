package com.griddynamics.customtype;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTimeWritable implements Writable {

	public DateTimeWritable() {
	}

	private Text beginTimestampText;
	private Text endTimestampText;
	private Long sessionDuration;

	public Text getBeginTimestampText() {
		return beginTimestampText;
	}

	public void setBeginTimestampText(Text beginTimestampText) {
		this.beginTimestampText = beginTimestampText;
	}

	public Text getEndTimestampText() {
		return endTimestampText;
	}

	public void setEndTimestampText(Text endTimestampText) {
		this.endTimestampText = endTimestampText;
	}

	public Long getSessionDuration() {
		return sessionDuration;
	}

	public void setSessionDuration(Long sessionDuration) {
		this.sessionDuration = sessionDuration;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBytes(this.beginTimestampText.toString());
		dataOutput.writeBytes(this.endTimestampText.toString());
		dataOutput.writeLong(this.sessionDuration);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.beginTimestampText = new Text(Text.readString(dataInput));
		this.endTimestampText = new Text(Text.readString(dataInput));
		this.sessionDuration = dataInput.readLong();
	}

	@Override
	public String toString(){
		return this.beginTimestampText.toString()+","+this.endTimestampText.toString()+","+String.valueOf(this.sessionDuration);
	}
}
