package org.hackreduce.examples.ngram.two_gram;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.ngram.TwoGramMapper;
import org.hackreduce.models.ngram.TwoGram;


/**
 * This MapReduce job will count the total number of {@link TwoGram} records in the data dump.
 *
 */
public class HackRecordCounter extends org.hackreduce.examples.RecordCounter {

	public enum Count {
		TOTAL_RECORDS
	}

	public static class HackRecordCounterMapper extends TwoGramMapper<Text, LongWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

		@Override
		protected void map(TwoGram record, Context context) throws IOException, InterruptedException {
			// Count only anagrams by returning early if not an anagram

			// Simple length check should usually suffice
			//if (record.getGram1().length() != record.getGram2().length()) return;

			// Same length, so sort lowercase characters to compare
			//char[] g1 = record.getGram1().toLowerCase().toCharArray();
			//char[] g2 = record.getGram2().toLowerCase().toCharArray();
			//Arrays.sort(g1);
			//Arrays.sort(g2);
			//if (!Arrays.equals(g1, g2)) return;

			// Write out count of one, keyed by the canonicalized gram
			//context.write(new Text(new String(g1)), new LongWritable(record.getMatchCount()));
			//context.getCounter(Count.TOTAL_RECORDS).increment(1);
			//context.write(TOTAL_COUNT, ONE_COUNT);

			// Count alliteration by returning early if not matching first letter
			String g1 = record.getGram1();
			String g2 = record.getGram2();
			if (g2 == null || g1.length() < 3 || g2.length() < 3) return;
			if (!g1.matches("[a-zA-Z]+") || !g2.matches("[a-zA-Z]+")) return;
			char c1 = g1.charAt(0);
			char c2 = g2.charAt(0);
			if (!Character.isLetter(c1) || !Character.isLetter(c2)) return;
			c1 = Character.toUpperCase(c1);
			c2 = Character.toUpperCase(c2);
			if (c1 != c2) return;
			// Write alliteration by year, letter, count
			context.write(
				new Text("" + c1 + " " + Integer.toString(record.getYear())),
				new LongWritable(record.getMatchCount()));
		}
	}

	@Override
	public void configureJob(Job job) {
		// The flight data dumps comes in as a CSV file (text input), so we configure
		// the job to use this format.
		TwoGramMapper.configureJob(job);
	}

	@Override
	public Class<? extends ModelMapper<?, ?, ?, ?, ?>> getMapper() {
		return HackRecordCounterMapper.class;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new HackRecordCounter(), args);
		System.exit(result);
	}
}
