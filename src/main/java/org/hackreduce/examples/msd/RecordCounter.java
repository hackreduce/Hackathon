package org.hackreduce.examples.msd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.MSDMapper;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.models.MSD;


/**
 * This MapReduce job will count the total number of {@link MSD} records in the data dump.
 *
 */
public class RecordCounter extends org.hackreduce.examples.RecordCounter {

    public enum Count {
        TOTAL_RECORDS
    }

    public static class RecordCounterMapper extends MSDMapper<Text, LongWritable> {

        public static final Text TOTAL_COUNT = new Text("total");
        public static final LongWritable ONE_COUNT = new LongWritable(1);

        @Override
        protected void map(MSD record, Context context) throws IOException, InterruptedException {
            context.getCounter(Count.TOTAL_RECORDS).increment(1);
            context.write(TOTAL_COUNT, ONE_COUNT);
        }

    }

    @Override
    public void configureJob(Job job) {
        MSDMapper.configureJob(job);
    }

    @Override
    public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
        return RecordCounterMapper.class;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new RecordCounter(), args);
        System.exit(result);
    }
}