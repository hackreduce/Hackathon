package org.hackreduce.examples.bixi;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.BixiMapper;
import org.hackreduce.models.BixiRecord;


/**
 * This MapReduce job will count the total number of Bixi records in the data dump.
 *
 */
public class Squasher extends Configured implements Tool {

	private static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd_MM_yyyy");
	
	public enum Count {
		TOTAL_RECORDS,
		SQUASHED
	}
	
	public static class StationMapper extends BixiMapper<Text, Text> {
		
		@Override
		protected void map(BixiRecord station, Context context) throws IOException,
				InterruptedException {
			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			context.write(new Text(SIMPLE_DATE_FORMAT.format(station.getDate())), new Text(station.toXML()));
		}

	}
	
	public static class StationReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		protected void reduce(Text day, Iterable<Text> stations, Context context) 
		throws IOException, InterruptedException {
			context.write(new Text("<stations>"), NullWritable.get());
			for (Text station : stations) {
				context.write(station, NullWritable.get());
			}
			context.getCounter(Count.SQUASHED).increment(1);
			context.write(new Text("</stations>"), NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new Squasher(), args);
		System.exit(result);
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
        job.setMapperClass(StationMapper.class);
		job.setReducerClass(StationReducer.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    // FIXME: why is the configure method on the mapper???
	    BixiMapper.configureJob(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
