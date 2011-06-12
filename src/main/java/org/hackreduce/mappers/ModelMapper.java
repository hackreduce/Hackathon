package org.hackreduce.mappers;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.models.StockExchangeRecord;

/**
 * Extends the basic Hadoop {@link Mapper} to process the NASDAQ/NYSE data dump by accessing {@link StockExchangeRecord}
 *
 * @param <M> Model that will be mapped
 * @param <IK> Input key type
 * @param <IV> Input value type
 * @param <K> Output key type
 * @param <V> Output value type
 */
public abstract class ModelMapper<M, IK, IV, K extends WritableComparable<?>, V extends Writable>
extends Mapper<IK, IV, K, V> {

	Logger LOG = Logger.getLogger(ModelMapper.class.getName()); 
	
	public enum ModelMapperCount {
		RECORDS_SKIPPED,
		RECORDS_MAPPED
	}

	protected abstract M instantiateModel(IK key, IV value);

	protected abstract void map(M record, Context context) throws IOException, InterruptedException;

	@Override
	protected void map(IK key, IV value, Context context) throws IOException, InterruptedException {
		M record = null;

		try {
			record = instantiateModel(key, value);
		} catch (Exception e) {
			LOG.log(Level.WARNING, e.getMessage(), e);
			context.getCounter(ModelMapperCount.RECORDS_SKIPPED).increment(1);
		}

		if (record != null) {
			context.getCounter(ModelMapperCount.RECORDS_MAPPED).increment(1);
			map(record, context);
		}
	}

}
