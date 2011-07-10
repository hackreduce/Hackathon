package org.hackreduce.models;

import java.util.logging.Logger;

import org.apache.hadoop.io.Text;

public class FreebaseQuadRecord {

	Logger LOG = Logger.getLogger(FreebaseQuadRecord.class.getName());

	private String source;
	private String property;
	private String destination;
	private String value;

	public FreebaseQuadRecord(String string) throws IllegalArgumentException {
		String[] pieces = string.split("\t");
		source = pieces[0];
		property = pieces[1];
		if (pieces.length > 2) {
			destination = pieces[2];
			// TODO: Extend this to interpret destination/value?

			// case #1: destination column holds a mid:
			// case #2: destination column empty, value column holds a value (int,
			// float, boolean, other?)
			// case #3: both destination and value columns hold values - namespace/key, lang/text
			if (destination.startsWith("/lang")) {
				// value column contains a string literal
			}
		} else {
			LOG.warning("Short record: " + string);
		}
		if (pieces.length > 3) {
			value = pieces[3];
		}

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

	public String toTSV() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(getSource()).append('\t');
		buffer.append(getProperty()).append('\t');
		buffer.append(getDestination()).append('\t');
		buffer.append(getValue()).append('\t');
		return buffer.toString();
	}

}
