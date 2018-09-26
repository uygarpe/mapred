package com.griddynamics.reducer;

import com.griddynamics.customtype.CompositeGroupKey;
import com.griddynamics.customtype.TimestampWritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Combiner class that holds the reduce which flattens data produced by UserSessionInfoMapper to be used by SessionDurationReducer.
 */
public class SessionDataFlattener extends Reducer<CompositeGroupKey, TimestampWritableComparable,CompositeGroupKey,TimestampWritableComparable> {
	/**
	 * Overriden reduce method that flattens data in the form:
	 * <table summary="">
	 *   <tr>
	 *     <th>username</th>
	 *     <th>sessionId</th>
	 *     <th>beginTimestamp</th>
	 *     <th>endTimestamp</th>
	 *     <th>duration</th>
	 *   </tr>
	 *   <tr>
	 *     <td>user1</td>
	 *     <td>0949afee</td>
	 *     <td>2018-08-03T10:15:30</td>
	 *     <td></td>
	 *     <td>0</td>
	 *   </tr>
	 *   <tr>
	 *     <td>user1</td>
	 *     <td>0949afee</td>
	 *     <td></td>
	 *     <td>2018-08-03T12:15:30</td>
	 *     <td>0</td>
	 *   </tr>
	 * </table>
	 * to <i>"user1","2018-08-03T10:15:30","2018-08-03T12:15:30",7200</i>.
	 * Removes sessionId for reducer to group over username only.
	 * @param key CompositeGroupKey
	 * @param values TimestampWritableComparable values for the given CompositeGroupKey
	 * @param context Object to write out results
	 * @throws IOException in case of filesystem io errors
	 * @throws InterruptedException in case the task is interrupted by user input or otherwise.
	 */

	@Override
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
		// Remove sessionId to group over username
		key.setSessionId("");
		context.write(key, result);
	}
}