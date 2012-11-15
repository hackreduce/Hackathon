package org.hackreduce.models;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;

/**
 * Record for a topic from the Freebase topic dump.
 * Each line in the file represents a single topic as described here:
 * http://wiki.freebase.com/wiki/Data_dumps#Simple_Topic_Dump
 * <p>
 * The dumps themselves are available from: <ul>
 * <li>http://download.freebase.com/datadumps/latest/freebase-simple-topic-dump.tsv.bz2 (updated weekly)</li>
 * <li>http://aws.amazon.com/datasets/8247878934976180 (updated occasionally)</li>
 * 
 * The simple topic dump is 1.4 GB compressed and 5.4 GB uncompressed (November 2012).
 * 
 * Note that unless you are using Hadoop 0.23.x or 2.x the bzip2 codec isn't
 * splittable, so you'll need to decompression the dump before processing.
 * 
 * @author Tom Morris <tfmorris@gmail.com>
 * 
 */
public class FreebaseTopicRecord {

	Logger LOG = Logger.getLogger(FreebaseTopicRecord.class.getName());
	
	private String mid;
	private String name;
	private String[] ids;
	private String[] wp_ids;
	private String[] fb_types;
	private String blurb;
	

	/**
	 * Construct a FreebaseTopicRecord object from a single line of text
	 * as read from the simple topic dump.
	 * 
	 * @param text the line of text 
	 * @throws IllegalArgumentException
	 */
	public FreebaseTopicRecord(Text text) throws IllegalArgumentException {
		this(text.toString());
	}
	
	/**
	 * Construct a FreebaseTopicRecord object from a single line of text
	 * as read from the simple topic dump.
	 * 
	 * @param string the line of text 
	 * @throws IllegalArgumentException
	 */
	public FreebaseTopicRecord(String string) throws IllegalArgumentException {
		String[] pieces = string.split("\t");
		mid = pieces[0];
		name = unescape(pieces[1]);
		ids = split(pieces[2]);
		wp_ids = split(pieces[3]);
		fb_types = split(pieces[4]);
		blurb = unescape(pieces[5]);
	}

	private static String unescape(String s) {
		// \N is a special signal value representing a null
		if ("\\N".equals(s)) {
			return null;
		}
		// tabs, newlines, and backslashes are the only other escaped characters
		return s.replaceAll("\\t","\t").replaceAll("\\n","\n").replaceAll("\\\\", "\\");
	}

	private static String escape(String s) {
		// \N is a special signal value representing a null
		if (s == null) {
			return "\\N";
		}
		// Escape tabs, newlines, and backslashes
		return s.replaceAll("\\","\\\\").replaceAll("\t","\\t").replaceAll("\n","\\n");
	}

	// Split a comma separated string of ids
	private static String[] split(String s) {
		if ("\\N".equals(s)) {
			return new String[0];
		}
		return s.split(",");
	}


	/**
	 * @return the primary id (MID) of the Freebase topic which this
	 * record represents.
	 */
	public String getMid() {
    	return mid;
    }
	
	/**
	 * @return the primary name of the Freebase topic
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return an array of Freebase IDs
	 */
	public String[] getIds() {
		return ids;
	}


	/**
	 * @return An array of numeric Wikipedia article ids as strings
	 */
	public String[] getWp_ids() {
		return wp_ids;
	}


	/**
	 * @return an array of Freebase type ids representing all the types
	 * which have been applied to this topic
	 */
	public String[] getFb_types() {
		return fb_types;
	}


	/**
	 * @return A short textual description of the topic.  For Wikipedia-
	 * derived topics, this is typically the first paragraph or so of the
	 * Wikipedia article.  As such, it falls under the Wikipedia license,
	 * not the Freebase license.
	 */
	public String getBlurb() {
		return blurb;
	}


	/**
	 * Format the record into a string in the same format as the input.
	 * 
	 * @return a String representing the line of text
	 */
	public String toTSV() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(getMid()).append('\t');
		buffer.append(getName()).append('\t');
		buffer.append(toCSV(getIds())).append('\t');
		buffer.append(toCSV(getWp_ids())).append('\t');
		buffer.append(toCSV(getFb_types())).append('\t');
		buffer.append(escape(getBlurb())).append('\t');
		return buffer.toString();
	}	

	private static String toCSV(String[] args) {
		return join(Arrays.asList(args),",");
	}

	private static String join(List<? extends CharSequence> s, String delimiter) {
		int capacity = 0;
		int delimLength = delimiter.length();
		Iterator<? extends CharSequence> iter = s.iterator();
		if (iter.hasNext()) {
			capacity += iter.next().length() + delimLength;
		}

		StringBuilder buffer = new StringBuilder(capacity);
		iter = s.iterator();
		if (iter.hasNext()) {
			buffer.append(iter.next());
			while (iter.hasNext()) {
				buffer.append(delimiter);
				buffer.append(iter.next());
			}
		}
		return buffer.toString();
	}


}
