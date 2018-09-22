package com.griddynamics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<Object, Text, CompositeGroupKey, TimestampWritableComparable> {

	private static final String BEGIN = "begin";
	private static final String END = "end";

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().length() > 0) {
			String userActions[] = value.toString().split(",");
			long beginTimestamp=0L;
			long endTimestamp=0L;

			if(userActions[2].equals(BEGIN)){
				beginTimestamp = Long.valueOf(userActions[3]);
			}else{
				endTimestamp = Long.valueOf(userActions[3]);
			}
			TimestampWritableComparable tsw = new TimestampWritableComparable(beginTimestamp,endTimestamp);
			context.write(new CompositeGroupKey(userActions[0], userActions[1]),tsw);

		}
	}
}

