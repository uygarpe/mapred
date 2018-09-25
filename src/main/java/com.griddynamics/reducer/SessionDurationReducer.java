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

public class SessionDurationReducer	extends Reducer<CompositeGroupKey,TimestampWritableComparable,NullWritable,Text> {

	DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

	private LocalDateTime epochSecondsToLocalDateTime(long seconds) {
		Instant instant = Instant.ofEpochMilli(seconds*1000);
		LocalDateTime date = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
		return date;
	}

	@Override
	public void reduce(CompositeGroupKey key, Iterable<TimestampWritableComparable> values,
	                   Context context
	) throws IOException, InterruptedException {
		DateTimeWritable result = new DateTimeWritable();
		long maxdur= 0L;
		LocalDateTime beginLocalDate=null;
		LocalDateTime endLocalDate=null;
		for (TimestampWritableComparable val : values) {
			long dur = val.getSessionDuration();
			if(dur > maxdur){
				maxdur= dur;
				beginLocalDate = epochSecondsToLocalDateTime(val.getBeginTimestamp());
				endLocalDate = epochSecondsToLocalDateTime(val.getEndTimestamp());
			}
		}
		result.setBeginTimestampText(new Text(beginLocalDate.format(formatter)));
		result.setEndTimestampText(new Text(endLocalDate.format(formatter)));
		result.setSessionDuration(maxdur);
		context.write(NullWritable.get(), new Text(key.getUsername().concat(",").concat(result.toString())));
	}
}
