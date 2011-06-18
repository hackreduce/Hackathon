package org.thebigjc.hackreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.StockExchangeMapper;



/**
 * Linear regression of a stock pair. 
 * Based on http://en.wikipedia.org/wiki/Simple_linear_regression#Linear_regression_without_the_intercept_term
 */
public class NoInterceptRegression extends Configured implements Tool {

	public enum Count {
		PAIRS
	}

	public static class NoInterceptMapper extends StockPairMapper<Text, Text>  {
		@Override
		protected StockPairRecord instantiateModel(LongWritable key, Text value) {
			return new StockPairRecord(value);
		}

		static String join(String delimiter, String ... s) {
		     StringBuilder builder = new StringBuilder();
		     Iterator<String> iter = Arrays.asList(s).iterator();
		     while (iter.hasNext()) {
		         builder.append(iter.next());
		         if (!iter.hasNext()) {
		           break;                  
		         }
		         builder.append(delimiter);
		     }
		     return builder.toString();
		 }


		@Override
		protected void map(StockPairRecord record, Context context) throws IOException, InterruptedException {
			String keyStr = record.getPairKey();
			// Let's just do all time for now - we could use this to do windowing
			
			context.write(new Text(keyStr), new Text(record.getValue()));
		}
	}

	public static class NoInterceptReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.PAIRS).increment(1);
			double numer = 0.0;
			double denom = 0.0;
			
			for (Text t : values) {
				double vals[] = toDoubles(t.toString().split("\\|"));
				double x = vals[0];
				double y = vals[1];
				
				numer += x*y;
				denom += x*x;
			}
			
			double beta = numer/denom;
			
			context.write(key, new DoubleWritable(beta));
		}

		private double[] toDoubles(String[] split) {
			double doubles[] = new double[split.length];
			int i = 0;
	
			for (String s : split) {
				doubles[i] = Double.parseDouble(s);
				i++;
			}
			return doubles;
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
        job.setMapperClass(NoInterceptMapper.class);
		job.setReducerClass(NoInterceptReducer.class);

		// Configure the job to accept the NASDAQ/NYSE data as input
		StockExchangeMapper.configureJob(job);
		
		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
		job.getConfiguration().setInt("mapred.reduce.tasks", 30);
		
		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new NoInterceptRegression(), args);
		System.exit(result);
	}

}
