package org.hackreduce.mappers.ngram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.ngram.TwoGram;


/**
 * Extends the basic Hadoop {@link Mapper} to process the 2gram data dump by
 * accessing {@link TwoGram}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class TwoGramMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<TwoGram, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the NASDAQ dump from Infochimps.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected TwoGram instantiateModel(LongWritable key, Text value) {
		return new TwoGram(value);
	}

}