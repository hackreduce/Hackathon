package org.hackreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.models.StockExchangeRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the NASDAQ/NYSE daily prices data dump by
 * accessing {@link StockExchangeRecord}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class StockExchangeMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<StockExchangeRecord, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the NASDAQ dump from Infochimps.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected StockExchangeRecord instantiateModel(LongWritable key, Text value) {
		return new StockExchangeRecord(value);
	}

}