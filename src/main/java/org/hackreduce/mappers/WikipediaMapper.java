package org.hackreduce.mappers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.models.WikipediaRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the Wikipedia XML data dump by
 * using {@link WikipediaRecord}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class WikipediaMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<WikipediaRecord, Text, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the Wikipedia XML data dump.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(XMLInputFormat.class);
		XMLRecordReader.setRecordTags(job, "<page>", "</page>");
	}

	@Override
	protected WikipediaRecord instantiateModel(Text xmlFilename, Text xml) {
		return new WikipediaRecord(xml);
	}

}