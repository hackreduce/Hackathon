package org.hackreduce.examples.nasdaq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.NasdaqMapper;
import org.hackreduce.models.NasdaqRecord;


/**
 * This MapReduce job will count the total number of NASDAQ records stored within the
 * files of the given input directories.
 *
 * It's meant to be an explicit example to show all the moving parts of MapReduce job.
 *
 */
public class NasdaqSymbolCounter extends Configured implements Tool {

	public static final LongWritable ONE_COUNT = new LongWritable(1);

	public enum Count {
		STOCK_SYMBOLS
	}


	public static class NasdaqSymbolCounterMapper extends NasdaqMapper<Text, LongWritable> {

		@Override
		protected void map(NasdaqRecord record, Context context) throws IOException, InterruptedException {
			context.write(new Text(record.getStockSymbol()), ONE_COUNT);
		}

	}

	public static class NasdaqSymbolCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.STOCK_SYMBOLS).increment(1);

			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}

			context.write(key, new LongWritable(count));
		}

	}

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // Parsing Hadoop arguments
        String[] jobArguments = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Anything not parsed belongs to the job itself
        if (jobArguments.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(NasdaqSymbolCounterMapper.class);
		job.setReducerClass(NasdaqSymbolCounterReducer.class);

		// Configure the job to accept the Nasdaq data as input
		NasdaqMapper.configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(jobArguments[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(jobArguments[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new NasdaqSymbolCounter(), args);
		System.exit(result);
	}

}
