package org.hackreduce.examples.hansard;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
import org.hackreduce.mappers.HansardMapper;
import org.hackreduce.models.HansardRecord;


/**
 * This MapReduce job will count the total number of Hansard records in the data dump.
 *
 */
public class SwearsByMember extends Configured implements Tool {

	public enum Count {
		TOTAL_RECORDS,
		TOTAL_FAIL
	}
	
	public static List<String> SWEAR_WORDS = Arrays.asList(
		"arse", "ass", "asshole", "bastard", "bitch", "bloody", "blowjob", "bollock", "boner", "boob",
		"boobs", "bugger", "bum", "butt", "cock", "crap", "cunt", "damn", "darn", "dick", "dumb", "dyke", "fag",
		"fuck", "fucking", "fucker", "fucked", "goddam", "hell", "homo", "idiot", "jerk", "jerkoff",
		"piss", "prick", "pussy", "queer", "shit", "shitty", "stupid", "slut", "tit", "tits",
		"turd", "twat", "whore");
	
	public static class SwearsByMemberMapper extends HansardMapper<Text, Text> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		@Override
		protected void map(HansardRecord record, Context context) throws IOException,
				InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1);
			String who = record.getWho();
			if (who.contains("(")) {
				who = who.split("\\(")[0];
			}
			// write each token for member
			try {
				List<String> tokens = Arrays.asList(
						record.getText().toLowerCase().split("\\W|_"));
				for(String token : tokens) {
					context.write(new Text(Integer.toString(record.getMemberId())), 
							new Text(token));
				}
				
			} catch (Exception e) {
				context.getCounter(Count.TOTAL_FAIL).increment(1);
			}
		}
	}
	
	public static class SwearsByMemberReducer extends Reducer<Text, Text, Text, Text> {

		public enum Count {
			TOTAL_TOKENS,
			TOTAL_SWEARS
		}
		
		protected void reduce(Text member, java.lang.Iterable<Text> tokens, Context context) 
				throws IOException, InterruptedException {
			int swearCount = 0;
			int tokenCount = 0;
			StringBuffer wordString = new StringBuffer();
			for (Text text: tokens) {
				tokenCount++;
				String token = text.toString();
				// match tokens against swearwords
				for (String swearword : SWEAR_WORDS) {
					if (token.equals(swearword)) {
						swearCount++;
						if (wordString.length() > 0) {
							wordString.append(",");
						}
						wordString.append(swearword);
					}
				}
			}
		
			// Write the swear to token ratio for this member
			context.write(member, new Text(swearCount + "\t" + tokenCount + "\t" + new Float(swearCount)/tokenCount + "\t" + wordString));
			context.getCounter(Count.TOTAL_SWEARS).increment(swearCount);
			context.getCounter(Count.TOTAL_TOKENS).increment(tokenCount);
		}
	}
	

	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(SwearsByMemberMapper.class);
		job.setReducerClass(SwearsByMemberReducer.class);

		// Configure the job to accept the Hansard data as input
		HansardMapper.configureJob(job);

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
		int result = ToolRunner.run(new Configuration(), new SwearsByMember(), args);
		System.exit(result);
	}
}
