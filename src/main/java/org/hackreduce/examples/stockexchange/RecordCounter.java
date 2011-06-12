package org.hackreduce.examples.stockexchange;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * This MapReduce job will count the total number of NASDAQ or NYSE records stored within the
 * files of the given input directories. It will also skip the CSV header.
 *
 * It's meant to be an explicit example to show all the moving parts of a MapReduce job. Much of
 * the code is just copied from the models and mapper package.
 *
 */
public class RecordCounter extends Configured implements Tool {

	public enum Count {
		RECORDS_SKIPPED,
		TOTAL_KEYS,
		UNIQUE_KEYS
	}

	public static class RecordCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		@Override
		@SuppressWarnings("unused")
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String inputString = value.toString();

			try {
				// This code is copied from the constructor of StockExchangeRecord

				String[] attributes = inputString.split(",");

				if (attributes.length != 9)
					throw new IllegalArgumentException("Input string given did not have 9 values in CSV format");

				try {
					String exchange = attributes[0];
					String stockSymbol = attributes[1];
					Date date = sdf.parse(attributes[2]);
					double stockPriceOpen = Double.parseDouble(attributes[3]);
					double stockPriceHigh = Double.parseDouble(attributes[4]);
					double stockPriceLow = Double.parseDouble(attributes[5]);
					double stockPriceClose = Double.parseDouble(attributes[6]);
					int stockVolume = Integer.parseInt(attributes[7]);
					double stockPriceAdjClose = Double.parseDouble(attributes[8]);
				} catch (ParseException e) {
					throw new IllegalArgumentException("Input string contained an unknown value that couldn't be parsed");
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed");
				}
			} catch (Exception e) {
				context.getCounter(Count.RECORDS_SKIPPED).increment(1);
				return;
			}

			context.getCounter(Count.TOTAL_KEYS).increment(1);
			context.write(TOTAL_COUNT, ONE_COUNT);
		}

	}

	public static class RecordCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.UNIQUE_KEYS).increment(1);

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

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(RecordCounterMapper.class);
		job.setReducerClass(RecordCounterReducer.class);

		// The Nasdaq/NYSE data dumps comes in as a CSV file (text input), so we configure
		// the job to use this format.
		job.setInputFormatClass(TextInputFormat.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new RecordCounter(), args);
		System.exit(result);
	}

}
