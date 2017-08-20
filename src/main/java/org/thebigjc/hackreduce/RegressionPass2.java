package org.thebigjc.hackreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Most code borrowed from
 * http://introcs.cs.princeton.edu/java/97data/LinearRegression.java.html
 */
public class RegressionPass2 extends Configured implements Tool {

	public enum Count {
		PAIRS
	}

	public static class RegressionPass2Mapper extends StockPairMapper<Text, Text>  {
		private HashMap<String, Double> xbar;
		private HashMap<String, Double> xbar2;
		
		
		@Override
		protected StockPairRecord instantiateModel(LongWritable key, Text value) {
			return new StockPairRecord(value);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			
			xbar = new HashMap<String, Double>();
			xbar2 = new HashMap<String, Double>();
			
			FSDataInputStream fdis = fs.open(new Path("/users/team9/xbar.out"));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
			String line;
			while ((line = reader.readLine()) != null) {
				String vals[] = line.split("\t");
				xbar.put(vals[0], Double.parseDouble(vals[1]));
				xbar2.put(vals[0], xbar.put(vals[0], Double.parseDouble(vals[2])));
			}
			
			reader.close();
			
			super.setup(context);
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
			double xbar = findSymbolBar(record.getClose1().getTicker());
			double ybar = findSymbolBar(record.getClose2().getTicker());
			double xbar2 = findSymbolBar2(record.getClose1().getTicker());
			
			context.write(new Text(keyStr), 
						new Text(
									join("\t", record.toString().replace("|", "\t"), 
											Double.toString(xbar), 
											Double.toString(ybar), 
											Double.toString(xbar2))));
		}

		private double findSymbolBar2(String ticker) {
			return xbar2.get(ticker);
		}

		private double findSymbolBar(String ticker) {
			return xbar.get(ticker);
		}
	}

	public static class RegressionPass2Reducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.PAIRS).increment(1);
			double xxbar = 0.0;
			double yybar = 0.0;
			double xybar = 0.0;
			
			double xbar = 0.0;
			double ybar = 0.0;
			
			for (Text t : values) {
				double vals[] = toDoubles(t.toString().split("\t"));
				double x = vals[0];
				double y = vals[1];
				xbar = vals[2];
				ybar = vals[3];
				
				xxbar += (x - xbar) * (x - xbar);
				yybar += (y - ybar) * (y - ybar);
				xybar += (x - xbar) * (y - ybar);
			}
			
			double beta1 = xybar / xxbar;
			double beta0 = ybar - beta1 * xbar;
			
			context.write(key, new Text(beta1 + "\t" + beta0));
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
        job.setMapperClass(RegressionPass2Mapper.class);
		job.setReducerClass(RegressionPass2Reducer.class);

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
		int result = ToolRunner.run(new Configuration(), new RegressionPass2(), args);
		System.exit(result);
	}

}
