package org.hackreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.models.FreebaseTopicRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the Freebase simple topic
 * dump by accessing {@link FreebaseTopicRecord}.
 * 
 * @param <K>
 *            Output class of the mapper key
 * @param <V>
 *            Output class of the mapper value
 * 
 */
public abstract class FreebaseTopicMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<FreebaseTopicRecord, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the Freebase simple topic dump.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected FreebaseTopicRecord instantiateModel(LongWritable key, Text text) {
		return new FreebaseTopicRecord(text);
	}

}