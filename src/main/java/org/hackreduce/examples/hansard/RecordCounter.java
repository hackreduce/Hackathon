package org.hackreduce.examples.hansard;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.HansardMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.HansardRecord;


/**
 * This MapReduce job will count the total number of Hansard records in the data dump.
 *
 */
public class RecordCounter extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS
	}

	public static class RecordCounterMapper extends HansardMapper<Text, LongWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
		protected void map(HansardRecord record, Context context) throws IOException,
				InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			context.write(TOTAL_COUNT, ONE_COUNT);
		}

	}

	@Override
	public void configureJob(Job job) {
		// The hansard data dumps comes in as a CSV file (text input), so we configure
		// the job to use this format.
		HansardMapper.configureJob(job);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return RecordCounterMapper.class;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new RecordCounter(), args);
		System.exit(result);
	}
}
