/*
 *  Copyright 2011. Thomas F. Morris
 *  Licensed under new BSD license
 *  http://www.opensource.org/licenses/bsd-license.php
 */
package org.hackreduce.examples.freebase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.FreebaseTopicMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.FreebaseTopicRecord;


/**
 * This MapReduce job will find all Freebase topic names with a special flag
 * character which indicates they may have been loaded using the wrong character
 * encoding.
 */
public class BadEncodingCounter extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS,
		BAD_ENCODINGS
	}

	public static class BadEncodingMapper extends FreebaseTopicMapper<Text, Text> {

		private static final String FLAG_CHAR = "√";

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
		protected void map(FreebaseTopicRecord record, Context context)
		        throws IOException, InterruptedException {
			if (record != null) {
				String name = record.getName();
				if (name != null && name.contains(FLAG_CHAR)) {
					context.getCounter(Count.BAD_ENCODINGS).increment(1);
					context.write(new Text(record.getMid()), new Text(name));
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new BadEncodingCounter(), args);
		System.exit(result);
	}

	@Override
	public void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(0);
		job.setMapOutputValueClass(Text.class);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return BadEncodingMapper.class;
	}

}
