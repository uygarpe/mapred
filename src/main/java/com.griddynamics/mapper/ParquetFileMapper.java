package com.griddynamics.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Converts standard MapReduce output to Parquet file.
 */
public class ParquetFileMapper extends Mapper<LongWritable, Text, Void, GenericRecord>  {

	public static final Schema AVRO_SCHEMA = new	Schema.Parser().parse(
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

	/**
	 * Overriden map method that takes a line of mapreduce output which is comma separated and converts to parquet format.
	 * @param key LongWritable object which holds the line number
	 * @param value Text object that holds the line of data
	 * @param context Object to write out the result.
	 * @throws IOException in case of filesystem io errors
	 * @throws InterruptedException in case the task is interrupted by user input or otherwise.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		record.put("username", line[0]);
		record.put("startEventTime", line[1]);
		record.put("endEventTime", line[2]);
		record.put("sessionLength", Long.valueOf(line[3]));
		// Line number as key not needed, just write out parquet file given as Avro schema.
		context.write(null, record);
	}


}
