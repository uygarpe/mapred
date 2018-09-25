package com.griddynamics;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class ParquetFile extends Mapper<LongWritable, Text, Void, GenericRecord>  {

	static final Schema AVRO_SCHEMA = new	Schema.Parser().parse(
					"{\n" +
						"	\"type\":	\"record\",\n" +
						"	\"name\":	\"testFile\",\n" +
						"	\"doc\":	\"output file for reducer\",\n" +
						"	\"fields\":\n" +
						"	[\n" +
						"			{\"name\": \"username\",	\"type\":	\"string\"},\n"+
						"			{\"name\":	\"startEventTime\", \"type\":	\"string\"},\n"+
						"			{\"name\":	\"endEventTime\", \"type\":	\"string\"},\n"+
						"			{\"name\":	\"sessionLength\", \"type\":	\"long\"}\n"+
						"	]\n"+
						"}\n");

	private	GenericRecord record = new GenericData.Record(AVRO_SCHEMA);

	@Override
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		record.put("username", line[0]);
		record.put("startEventTime", line[1]);
		record.put("endEventTime", line[2]);
		record.put("sessionLength", Long.valueOf(line[3]));
		context.write(null, record);
	}


}
