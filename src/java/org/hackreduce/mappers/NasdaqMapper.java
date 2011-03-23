package org.hackreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.models.NasdaqRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the NASDAQ data dump by accessing {@link NasdaqRecord}.
 *
 * @param <K> Output class of the key
 * @param <V> Output class of the value
 *
 */
public abstract class NasdaqMapper<K extends WritableComparable<?>, V extends Writable> extends Mapper<LongWritable, Text, K, V> {

	public enum Count {
		RECORDS_SKIPPED,
		RECORDS_MAPPED
	}

	/**
	 * Configures the MapReduce job to read data from the NASDAQ dump from Infochimps.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	protected abstract void map(NasdaqRecord record, Context context) throws IOException, InterruptedException;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		NasdaqRecord record = null;

		try {
			record = new NasdaqRecord(value);
		} catch (Exception e) {
			context.getCounter(Count.RECORDS_SKIPPED).increment(1);
		}

		if (record != null) {
			context.getCounter(Count.RECORDS_MAPPED).increment(1);

			map(record, context);
		}
	}

}