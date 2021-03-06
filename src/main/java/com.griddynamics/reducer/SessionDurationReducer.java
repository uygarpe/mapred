package com.griddynamics.reducer;

import com.griddynamics.customtype.CompositeGroupKey;
import com.griddynamics.customtype.DateTimeWritable;
import com.griddynamics.customtype.TimestampWritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Reducer class to find the longest session by user
 */
public class SessionDurationReducer	extends Reducer<CompositeGroupKey,TimestampWritableComparable,NullWritable,Text> {

	DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

	/**
	 * Converts seconds to LocalDateTime which is in UTC.
	 * @param seconds
	 * @return LocalDateTime
	 */
	private LocalDateTime epochSecondsToLocalDateTime(long seconds) {
		Instant instant = Instant.ofEpochMilli(seconds*1000);
		LocalDateTime date = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
		return date;
	}

	/**
	 * Overriden reduce method that takes CompositeGroupKey <i>(username,sessionId)</i> and TimeStampWritableComparable
	 * values and finds the longest session by user.
	 * @param key CompositeGroupKey
	 * @param values TimeStampWritableComparable
	 * @param context Writes the output as <i>username,startTimestamp,endTimestamp,sessionDuration</i>
	 * @throws IOException in case of filesystem io errors
	 * @throws InterruptedException in case the task is interrupted by user input or otherwise.
	 */

	@Override
	public void reduce(CompositeGroupKey key, Iterable<TimestampWritableComparable> values,
	                   Context context
	) throws IOException, InterruptedException {
		DateTimeWritable result = new DateTimeWritable();
		long maxdur= 0L;
		LocalDateTime beginLocalDate=null;
		LocalDateTime endLocalDate=null;
		// Iterate over values for given key
		for (TimestampWritableComparable val : values) {
			long dur = val.getSessionDuration();
			if(dur > maxdur){
				maxdur= dur;
				beginLocalDate = epochSecondsToLocalDateTime(val.getBeginTimestamp());
				endLocalDate = epochSecondsToLocalDateTime(val.getEndTimestamp());
			}
		}
		// Write results
		result.setBeginTimestampText(new Text(beginLocalDate.format(formatter)));
		result.setEndTimestampText(new Text(endLocalDate.format(formatter)));
		result.setSessionDuration(maxdur);
		// Key is the line number which we do not need, so get rid of it by using NullWritable, then concat results with ","
		context.write(NullWritable.get(), new Text(key.getUsername().concat(",").concat(result.toString())));
	}
}
