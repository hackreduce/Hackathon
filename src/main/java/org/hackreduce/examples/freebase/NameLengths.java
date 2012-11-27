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
 * This MapReduce job will examine the names of all Freebase topics
 * and produce counts of the number of topics with names of each length
 * (in both words and characters).
 *
 */
public class NameLengths extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS,
	}

	public static class NameLengthsMapper extends FreebaseTopicMapper<Text, LongWritable> {

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
		protected void map(FreebaseTopicRecord record, Context context)
		        throws IOException, InterruptedException {
			if (record != null) {
				String name = record.getName();
				if (name != null ) {
					int words = name.split(" ").length; // a simplistic definition of "word"
					context.getCounter(Count.TOTAL_RECORDS).increment(1);
					context.write(new Text("w"+Integer.valueOf(words).toString()), ONE_COUNT);
					context.write(new Text("c"+Integer.valueOf(name.length()).toString()), ONE_COUNT);
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new NameLengths(), args);
		System.exit(result);
	}

	@Override
	public void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return NameLengthsMapper.class;
	}

}
