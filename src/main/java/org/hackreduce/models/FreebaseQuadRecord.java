package org.hackreduce.models;

import java.util.logging.Logger;

import org.apache.hadoop.io.Text;

/**
 * Record for a tuple from the Freebase quad dump.
 * Each line in the file represents a single Subject/Predicate/Object tuple as described here:
 * http://wiki.freebase.com/wiki/Data_dumps#Quad_dump
 * <p>
 * The dumps themselves are available from: <ul>
 * <li>http://download.freebase.com/datadumps/latest/freebase-datadump-quadruples.tsv.bz2 (updated weekly)</li>
 * <li>http://aws.amazon.com/datasets/8247878934976180 (updated occasionally)</li>
 * 
 * The quad dump is 4.8 GB compressed. (November 2012).
 * 
 * Note that unless you are using Hadoop 0.23.x or 2.x the bzip2 codec isn't
 * splittable, so you'll need to decompression the dump before processing.
 * 
 * @author Tom Morris <tfmorris@gmail.com>
 * 
 */
public class FreebaseQuadRecord {

	Logger LOG = Logger.getLogger(FreebaseQuadRecord.class.getName());

	public enum Type {OBJECT,TEXT,KEY,DATE,NUMBER,URL,VALUE};
	
	private String source;
	private String property;
	private String destination;
	private String value;
	private String namespace;
	private Type type;

	public FreebaseQuadRecord(String string) throws IllegalArgumentException {
		String[] pieces = string.split("\t");
		source = pieces[0]; // MID of source object in Freebase
		property = pieces[1]; // ID of property (e.g. /type/object/type, not an MID)
		// case #1: destination column holds a mid:
		// case #2: destination column empty, value column holds a value (int,
		// float, boolean, machine-readable string, URL, ISO date, time, ISO datetime, other?)
		// case #3: both destination and value columns hold values - namespace/key, lang/text
		if (pieces.length > 3) {
			value = pieces[3];
			if (pieces[2].length() > 0) {
				if (pieces[2].charAt(0) == '/') {
					namespace = pieces[2];
					if (pieces[2].startsWith("/lang/")) {
						value = unescape(value);
						type = Type.TEXT;
					} else {
						type = Type.KEY;
					}
				} else {
					LOG.warning("Unexpected namespace value" + pieces[2]);
				}
			} else {
				type = Type.VALUE;
				// TODO: Extend this to interpret typed values? Really need schema info to do correctly
			}
		} else if (pieces.length > 2) {
			destination = pieces[2];
			type = Type.OBJECT; // MID of a target object
		} else {
			LOG.warning("Short record: " + string);
		}

	}
	private static String unescape(String s) {
		// \N is a special signal value representing a null
		if ("\\N".equals(s)) {  // Not used in quad dump?
			return null;
		}
		// tabs, newlines, and backslashes are the only other escaped characters
		return s.replaceAll("\\t","\t").replaceAll("\\n","\n").replaceAll("\\\\", "\\");
	}

	private static String escape(String s) {
		if (s == null) {
			return "";
		}
		// Escape tabs, newlines, and backslashes 
		return s.replaceAll("\t","\\t").replaceAll("\n","\\n").replaceAll("\\", "\\\\");
	}


	public FreebaseQuadRecord(Text text) throws IllegalArgumentException {
		this(text.toString());
	}

	public String getSource() {
		return source;
	}

	public String getProperty() {
		return property;
	}

	public String getDestination() {
		return destination;
	}

	public String getValue() {
		return value;
	}

	/**
	 * @return the namespace for a key or the language for a text literal
	 */
	public String getNamespace() {
		return namespace;
	}

	public Type getType() {
		return type;
	}

	public String toTSV() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(getSource()).append('\t');
		buffer.append(getProperty()).append('\t');
		if (destination != null) {
			buffer.append(getDestination());
		} 
		buffer.append('\t');
		if (getType() == Type.TEXT) {
			buffer.append(escape(getValue())).append('\t');			
		} else {
			buffer.append(getValue()).append('\t');
		}
		return buffer.toString();
	}

}
