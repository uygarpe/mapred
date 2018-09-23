package com.griddynamics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimestampWritableComparable  implements WritableComparable<TimestampWritableComparable> {
	private LocalDateTime beginTimestamp;
	private LocalDateTime endTimestamp;
	private Duration sessionDuration;
	private Text beginTimestampText;
	private Text endTimestampText;
	private Text sessionDurationText;

	public Text getBeginTimestampText() {
		return beginTimestampText;
	}

	public void setBeginTimestampText(Text beginTimestampText) {
		this.beginTimestampText = beginTimestampText;
	}

	public void setBeginTimestampText(LocalDateTime beginTimestamp) {
		this.beginTimestampText = new Text(beginTimestamp.format(formatter));
	}

	public Text getEndTimestampText() {
		return endTimestampText;
	}

	public void setEndTimestampText(Text endTimestampText) {
		this.endTimestampText = endTimestampText;
	}

	public void setEndTimestampText(LocalDateTime endTimestamp) {
		this.endTimestampText = new Text(endTimestamp.format(formatter));
	}

	public Text getSessionDurationText() {
		return sessionDurationText;
	}

	public void setSessionDurationText(Text durationText) {
		this.sessionDurationText = durationText;
	}

	public void setSessionDurationText(Duration duration) {
		this.sessionDurationText = new Text(duration.toString());
	}

	DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

	public TimestampWritableComparable(){
		this.beginTimestamp=LocalDateTime.MIN;
		this.endTimestamp= LocalDateTime.MIN;
		this.sessionDuration = Duration.ZERO;
	}

	public TimestampWritableComparable(LocalDateTime bt, LocalDateTime et){
		this.beginTimestamp=bt;
		this.endTimestamp= et;
		if (et.equals(LocalDateTime.MIN) || bt.equals(LocalDateTime.MIN)){
			this.sessionDuration=Duration.ZERO;
		}
		else {
			this.sessionDuration = Duration.between(et,bt);
		}
		this.setBeginTimestampText(bt);
		this.setEndTimestampText(et);
		this.setSessionDurationText(this.sessionDuration);
	}

	public LocalDateTime getBeginTimestamp() {
		return beginTimestamp;
	}

	public void setBeginTimestamp(LocalDateTime beginTimestamp) {
		this.beginTimestamp = beginTimestamp;
	}

	public LocalDateTime getEndTimestamp() {
		return endTimestamp;
	}

	public void setEndTimestamp(LocalDateTime endTimestamp) {
		this.endTimestamp = endTimestamp;
	}

	public Duration getSessionDuration() {
		return sessionDuration;
	}

	public void setSessionDuration(Duration sessionDuration) {
		this.sessionDuration = sessionDuration;
	}

	@Override
	public int compareTo(TimestampWritableComparable that) {
		return this.sessionDuration.compareTo(that.sessionDuration);

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBytes(beginTimestampText.toString());
		dataOutput.writeBytes(endTimestampText.toString());
		dataOutput.writeBytes(sessionDurationText.toString());

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		beginTimestampText.readFields(dataInput);
		endTimestampText.readFields(dataInput);
		sessionDurationText.readFields(dataInput);
		beginTimestamp = LocalDateTime.parse(beginTimestampText.toString(),formatter);
		endTimestamp = LocalDateTime.parse(endTimestampText.toString(),formatter);
		sessionDuration = Duration.parse(sessionDurationText.toString());
	}


	@Override
	public String toString() {
		return beginTimestamp.format(formatter) + "," + endTimestamp.format(formatter)+ "," +sessionDuration.toString();
	}


}
