/*
 *  Copyright 2011. Thomas F. Morris
 *  Licensed under new BSD license
 *  http://www.opensource.org/licenses/bsd-license.php
 */
package org.hackreduce.examples.freebase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.FreebaseTopicMapper;
import org.hackreduce.models.FreebaseTopicRecord;


/**
 * This MapReduce job will find all Freebase topic names with a special flag
 * character which indicates they may have been loaded using the wrong character
 * encoding.
 * <p>
 * This version is standalone and doesn't depend on RecordCounter class from
 * the examples.
 */
public class BadEncodingCounter2 extends Configured implements Tool {

	public enum Count {
		TOTAL_RECORDS,
		BAD_ENCODINGS
	}

	/**
	 * Mapper which outputs the MID (key) and name (value) of input records which 
	 * contain the flag character that we're searching for.
	 */
	public static class BadEncodingMapper extends FreebaseTopicMapper<Text, Text> {

		private static final String FLAG_CHAR = "√";

		@Override
		protected void map(FreebaseTopicRecord record, Context context)
		        throws IOException, InterruptedException {
			if (record != null) {
				String name = record.getName();
				if (name != null && name.contains(FLAG_CHAR)) {
					context.getCounter(Count.BAD_ENCODINGS).increment(1);
					context.write(new Text(record.getMid()), new Text(name));
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new BadEncodingCounter2(), args);
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

		job.setInputFormatClass(TextInputFormat.class);

        // Tell the job which Mapper to use
        job.setMapperClass(BadEncodingMapper.class);
        
        // No reducer, we just want to collect all the strings
        job.setNumReduceTasks(0);

		// This is what the Mapper will be outputting
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
