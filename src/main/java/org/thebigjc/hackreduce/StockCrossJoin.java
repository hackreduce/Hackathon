package org.thebigjc.hackreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.StockExchangeMapper;
import org.hackreduce.models.StockExchangeRecord;


/**
 * This MapReduce job will read the NASDAQ or NYSE daily prices dataset and output the highest market caps
 * obtained by each Stock symbol.
 *
 */
public class StockCrossJoin extends Configured implements Tool {

	public enum Count {
		DATES
	}

	public static class StockCrossJoinMapper extends StockExchangeMapper<Text, Text> {

		@Override
		protected void map(StockExchangeRecord record, Context context) throws IOException, InterruptedException {
			String dateStr = record.getDateString();
			context.write(new Text(dateStr), new Text(record.toString()));
		}
	}

	public static class StockCrossJoinReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.DATES).increment(1);

			// Let's hope there aren't too many stocks!
			ArrayList<StockExchangeRecord> sres = new ArrayList<StockExchangeRecord>();
			for (Text row : values) {
				StockExchangeRecord curS = new StockExchangeRecord(row);
				
				for (StockExchangeRecord sre: sres) {
					String s1 = sre.getStockSymbol();
					String s2 = curS.getStockSymbol();
					double d1 = sre.getStockPriceClose();
					double d2 = curS.getStockPriceClose();
					
					if (s1.compareTo(s2) < 0)
						writePair(key, context, s1, s2, d1, d2);
					else
						writePair(key, context, s2, s1, d2, d1);
				}
				
				sres.add(curS);
			}
		}

		private void writePair(Text date, Context context, String s1, String s2,
				double d1, double d2) throws IOException, InterruptedException {
			context.write(new Text(s1 + "|" + s2 + "|" + date), new Text(d1 + "|" + d2));
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
        job.setMapperClass(StockCrossJoinMapper.class);
		job.setReducerClass(StockCrossJoinReducer.class);

		// Configure the job to accept the NASDAQ/NYSE data as input
		StockExchangeMapper.configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new StockCrossJoin(), args);
		System.exit(result);
	}

}
