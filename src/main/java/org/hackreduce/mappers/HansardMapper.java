package org.hackreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.models.HansardRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the Hansard data dump by
 * using the {@link HansardRecord} model
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class HansardMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<HansardRecord, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the Hansard dump.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected HansardRecord instantiateModel(LongWritable key, Text value) {
		return new HansardRecord(value);
	}

}