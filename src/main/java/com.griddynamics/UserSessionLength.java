package com.griddynamics;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("Duplicates")
public class UserSessionLength {

	public static class SessionInfoFlattener	extends Reducer<CompositeGroupKey,TimestampWritableComparable,CompositeGroupKey,TimestampWritableComparable> {

		public void reduce(CompositeGroupKey key, Iterable<TimestampWritableComparable> values,
		                   Context context
		) throws IOException, InterruptedException {
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

	public static class SessionDurationReducer	extends Reducer<CompositeGroupKey,TimestampWritableComparable,CompositeGroupKey,DateTimeWritable> {

		DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

		private LocalDateTime epochSecondsToLocalDateTime(long seconds) {
			Instant instant = Instant.ofEpochMilli(seconds*1000);
			LocalDateTime date = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
			return date;
		}

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
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: usersessionlength <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "user session length");
		job.setJarByClass(UserSessionLength.class);

		job.setMapperClass(UserSessionInfoMapper.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(TimestampWritableComparable.class);

		job.setCombinerClass(SessionInfoFlattener.class);
		job.setReducerClass(SessionDurationReducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
