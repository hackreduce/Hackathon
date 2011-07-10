package org.hackreduce.models;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;

public class FreebaseTopicRecord {

	Logger LOG = Logger.getLogger(FreebaseTopicRecord.class.getName());
	
	private String mid;
	private String name;
	private String[] ids;
	private String[] wp_ids;
	private String[] fb_types;
	private String blurb;
	

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
		if ("\\N".equals(s)) {
			return null;
		}
		return s.replaceAll("\\t","\t").replaceAll("\\n","\n");
	}

	private static String[] split(String s) {
		if ("\\N".equals(s)) {
			return new String[0];
		}
		// TODO: Do we need to escape these ?  Probably not
//		s = s.replaceAll("\\t","\t").replaceAll("\\n","\n");
		return s.split(",");
	}
	
	public FreebaseTopicRecord(Text text) throws IllegalArgumentException {
		this(text.toString());
	}
	
	public String getName() {
		return name;
	}

	public String[] getIds() {
		return ids;
	}


	public String[] getWp_ids() {
		return wp_ids;
	}


	public String[] getFb_types() {
		return fb_types;
	}


	public String getBlurb() {
		return blurb;
	}


	public String toTSV() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(getName()).append('\t');
		buffer.append(toCSV(getIds())).append('\t');
		buffer.append(toCSV(getWp_ids())).append('\t');
		buffer.append(toCSV(getFb_types())).append('\t');
		buffer.append(getBlurb()).append('\t');
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

	public String getMid() {
    	return mid;
    }


}
