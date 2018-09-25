package com.griddynamics.mapper;

import com.griddynamics.customtype.CompositeGroupKey;
import com.griddynamics.customtype.TimestampWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;

public class UserSessionInfoMapper extends Mapper<Object, Text, CompositeGroupKey, TimestampWritableComparable> {

	private static final String BEGIN = "begin";
	private static final String END = "end";

	/**
	 * Overriden map method from Mapper class. Takes lines of data, parses them into format:
	 * (CompositeGroupKey, TimestampWritableComparable) tuples which translate to:
	 * <i>(username, sessionId, beginTimestamp, endTimestamp,Duration)</i>
	 * Uses TimestampWritableComparable objects to translate DateTime values of DateTimeFormatter.ISO_LOCAL_DATE_TIME
	 * format to milliseconds for comparison and find durations.
	 * @param key Index of the line of the file
	 * @param value one line of data
	 * @param context Mapper context that create output in the form of TimestampWritableComparable objects.
	 * @throws IOException in case of filesystem io errors
	 * @throws InterruptedException in case the task is interrupted by user input or otherwise.
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().length() > 0) {
			String userActions[] = value.toString().split(",");
			LocalDateTime beginTimestamp=LocalDateTime.MIN;
			LocalDateTime endTimestamp=LocalDateTime.MIN;

			if(userActions[2].equals(BEGIN)){
				beginTimestamp = LocalDateTime.parse(userActions[3]);
			}else{
				endTimestamp = LocalDateTime.parse(userActions[3]);
			}
			TimestampWritableComparable tsw = new TimestampWritableComparable(beginTimestamp,endTimestamp);
			context.write(new CompositeGroupKey(userActions[0], userActions[1]),tsw);

		}
	}
}

