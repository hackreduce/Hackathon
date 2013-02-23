package org.hackreduce.models;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;


/**
 * Parses a raw record (line of string data) from the Flight CSV data dump into a Java object.
 *
 */
public class HansardRecord {

	private static SimpleDateFormat DATE_PARSER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	String who;
	long sequence;
	String text;
	int politician_id;
	long wordcount;
	int hansard_id;
	String topic;
	boolean speaker;
	Date timestamp;
	int member_id;
	long statement_id;
	boolean written_question;
	String heading;
	
	public HansardRecord(String inputString) throws IllegalArgumentException {
		String[] attributes = inputString.replaceAll("\"","").split("\\|");

		if (attributes.length < 12)
			throw new IllegalArgumentException("Expecting 12 values, got " + attributes.length);

		try {
			setWho(attributes[0]);
			if (!attributes[1].isEmpty()) setSequence(Long.parseLong(attributes[1]));
			setText(attributes[2]);
			if (!attributes[3].isEmpty()) setPoliticianId(Integer.parseInt(attributes[3]));
			if (!attributes[4].isEmpty()) setWordcount(Long.parseLong(attributes[4]));
			setHansardId(Integer.parseInt(attributes[5]));
			setTopic(attributes[6]);
			setSpeaker(Boolean.parseBoolean(attributes[7]));
			try {
				setTimestamp(DATE_PARSER.parse(attributes[8]));
			} catch (ParseException e) {
				
				throw new IllegalArgumentException("Bad date format for date string: " + attributes[8]);
			}
			if (!attributes[9].isEmpty()) setMemberId(Integer.parseInt(attributes[9]));
			if (!attributes[10].isEmpty()) setStatementId(Long.parseLong(attributes[10]));
			if (!attributes[11].isEmpty()) setWrittenQuestion(Boolean.parseBoolean(attributes[11]));
			if (attributes.length > 12) setHeading(attributes[12]);
			
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
	}

	public HansardRecord(Text inputText) throws IllegalArgumentException {
		this(inputText.toString());
	}
	
	public String getWho() {
		return who;
	}

	public void setWho(String who) {
		this.who = who;
	}

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getPoliticianId() {
		return politician_id;
	}

	public void setPoliticianId(int politician_id) {
		this.politician_id = politician_id;
	}

	public long getWordcount() {
		return wordcount;
	}

	public void setWordcount(long wordcount) {
		this.wordcount = wordcount;
	}

	public int getHansardId() {
		return hansard_id;
	}

	public void setHansardId(int hansard_id) {
		this.hansard_id = hansard_id;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public boolean isSpeaker() {
		return speaker;
	}

	public void setSpeaker(boolean speaker) {
		this.speaker = speaker;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public int getMemberId() {
		return member_id;
	}

	public void setMemberId(int member_id) {
		this.member_id = member_id;
	}

	public long getStatementId() {
		return statement_id;
	}

	public void setStatementId(long statement_id) {
		this.statement_id = statement_id;
	}

	public boolean isWrittenQuestion() {
		return written_question;
	}

	public void setWrittenQuestion(boolean written_question) {
		this.written_question = written_question;
	}

	public String getHeading() {
		return heading;
	}

	public void setHeading(String heading) {
		this.heading = heading;
	}

}
