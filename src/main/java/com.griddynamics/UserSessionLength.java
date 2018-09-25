package com.griddynamics;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

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

	public static class SessionDurationReducer	extends Reducer<CompositeGroupKey,TimestampWritableComparable,NullWritable,Text> {

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
			context.write(NullWritable.get(), new Text(key.getUsername().concat(",").concat(result.toString())));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: usersessionlength <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "user session length");
		job1.setJarByClass(UserSessionLength.class);

		job1.setMapperClass(UserSessionInfoMapper.class);
		job1.setMapOutputKeyClass(CompositeGroupKey.class);
		job1.setMapOutputValueClass(TimestampWritableComparable.class);

		job1.setCombinerClass(SessionInfoFlattener.class);
		job1.setReducerClass(SessionDurationReducer.class);

		Path outputPath1=new Path(otherArgs[1]);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, outputPath1);
		job1.waitForCompletion(true);

		// Parquet conversion
		Configuration conf2=new Configuration();
		Job job2 = new Job(conf2, "write results as parquet");
		job2.setJarByClass(ParquetFile.class);
		job2.setMapperClass(ParquetFile.class);
		job2.setMapOutputKeyClass(GenericRecord.class);
		job2.setNumReduceTasks(0);
		job2.setOutputKeyClass(Void.class);
		job2.setOutputValueClass(Group.class);
		job2.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setSchema(job2, ParquetFile.AVRO_SCHEMA);
		Path outputPath2=new Path(args[2]);
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
