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
import org.hackreduce.mappers.FreebaseQuadMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.FreebaseQuadRecord;


/**
 * This MapReduce job will count the total number of assertions in the Freebase
 * quad dump.
 * 
 */
public class QuadCounter extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS,
		UNIQUE_KEYS
	}

	public static class RecordCounterMapper extends FreebaseQuadMapper<Text, LongWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
		protected void map(FreebaseQuadRecord record, Context context) throws IOException,
				InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			context.write(TOTAL_COUNT, ONE_COUNT);
		}

	}

	@Override
	public void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
//		job.setCombiner
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return RecordCounterMapper.class;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new QuadCounter(), args);
		System.exit(result);
	}

}
