package org.hackreduce.mappers.ngram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.ngram.OneGram;


/**
 * Extends the basic Hadoop {@link Mapper} to process the 1gram data dump by
 * accessing {@link OneGram}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class OneGramMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<OneGram, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the NASDAQ dump from Infochimps.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected OneGram instantiateModel(LongWritable key, Text value) {
		return new OneGram(value);
	}

}