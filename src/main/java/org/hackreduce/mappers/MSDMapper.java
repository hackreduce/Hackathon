package org.hackreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.models.MSD;

/**
 * Extends the basic Hadoop {@link Mapper} to process the Million Song Dataset data dump by
 * accessing {@link MSD}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class MSDMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<MSD, LongWritable, Text, K, V> {

    /**
     * Configures the MapReduce job to read data from the Bixi data dump.
     *
     * @param job
     */
    public static void configureJob(Job job) {
        job.setInputFormatClass(TextInputFormat.class);
    }

    @Override
    protected MSD instantiateModel(LongWritable key, Text value) {
        return new MSD(value);
    }
}