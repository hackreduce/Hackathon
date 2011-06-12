package org.hackreduce.mappers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.models.BixiRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the Bixi XML data dump by
 * accessing {@link BixiRecord}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class BixiMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<BixiRecord, Text, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the Bixi data dump.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(XMLInputFormat.class);
		XMLRecordReader.setRecordTags(job, "<station>", "</station>");
	}

	@Override
	protected BixiRecord instantiateModel(Text xmlFilename, Text xml) {
		return new BixiRecord(xmlFilename, xml);
	}

}