package com.griddynamics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
				bt += val.getBeginTimestamp();
				et += val.getEndTimestamp();
			}
			result.setBeginTimestamp(bt);
			result.setEndTimestamp(et);
			result.setSessionDuration(et-bt);
			key.setSessionId("");
			context.write(key, result);
		}
	}

	public static class SessionDurationReducer	extends Reducer<CompositeGroupKey,TimestampWritableComparable,CompositeGroupKey,TimestampWritableComparable> {

		public void reduce(CompositeGroupKey key, Iterable<TimestampWritableComparable> values,
		                   Context context
		) throws IOException, InterruptedException {
			TimestampWritableComparable result = new TimestampWritableComparable();
			long maxdur=0L;
			long bt=0;
			long et=0;
			for (TimestampWritableComparable val : values) {
				long dur = val.getSessionDuration();
				if(dur>maxdur){
					maxdur= dur;
					bt = val.getBeginTimestamp();
					et = val.getEndTimestamp();
				}
			}
			result.setBeginTimestamp(bt);
			result.setEndTimestamp(et);
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

		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(TimestampWritableComparable.class);

		job.setCombinerClass(SessionInfoFlattener.class);
		job.setReducerClass(SessionDurationReducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
