package com.griddynamics.reducer;

import com.griddynamics.customtype.CompositeGroupKey;
import com.griddynamics.customtype.TimestampWritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SessionDataFlattener extends Reducer<CompositeGroupKey, TimestampWritableComparable,CompositeGroupKey,TimestampWritableComparable> {

	public void reduce(CompositeGroupKey key, Iterable<TimestampWritableComparable> values, Context context)
					throws IOException, InterruptedException {
		TimestampWritableComparable result = new TimestampWritableComparable();
		long bt=0L;
		long et=0L;
		for (TimestampWritableComparable val : values) {
			if(bt <=0L) bt=val.getBeginTimestamp();
			if(et <=0L) et=val.getEndTimestamp();
		}
		result.setBeginTimestamp(bt);
		result.setEndTimestamp(et);
		result.setSessionDuration(bt,et);
		key.setSessionId("");
		context.write(key, result);
	}
}