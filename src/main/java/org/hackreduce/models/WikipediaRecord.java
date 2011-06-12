package org.hackreduce.models;

import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.Text;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class WikipediaRecord {

	int id;
	String title;
	int revisionId;
	Date revisionDate;
	String revisionComment;
	String text;

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	static {
		df.setTimeZone(TimeZone.getTimeZone("Zulu"));
	}


	public WikipediaRecord(String xml) throws IllegalArgumentException {
		// XML is like violence...
	    try {
	    	DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	    	InputSource is = new InputSource();
	        is.setCharacterStream(new StringReader(xml));
			Document doc = docBuilder.parse(is);

			NodeList pageList = doc.getElementsByTagName("page");
			if (pageList.getLength() != 1)
				throw new IllegalArgumentException();

			Element page = (Element)pageList.item(0);
			setTitle(getStringFromTag(page, "title"));
			setId(Integer.parseInt(getStringFromTag(page, "id")));

			NodeList revisionList = page.getElementsByTagName("revision");
			if (revisionList.getLength() < 1)
				throw new IllegalArgumentException();

			Element revision = (Element)revisionList.item(0);

			setRevisionId(Integer.parseInt(getStringFromTag(revision, "id")));
			setRevisionComment(getStringFromTag(revision, "comment"));

			String revisionDateString = getStringFromTag(revision, "timestamp");
			if (revisionDateString != null)
				setRevisionDate(df.parse(revisionDateString));

			setText(getStringFromTag(revision, "text"));
	    } catch (Exception e) {
	    	// Gotta catch 'em all!
			throw new IllegalArgumentException("Couldn't create a " + getClass().getName() + " record from the given XML");
		}
	    
	}

	private String getStringFromTag(Element rootElement, String tagname) {
		NodeList list = rootElement.getElementsByTagName(tagname);
		if (list.getLength() > 0) {
			Element element = (Element)list.item(0);
			Node child = element.getFirstChild();
			if (child instanceof CharacterData) {
				CharacterData cd = (CharacterData) child;
				return cd.getData();
			}
		}
		return null;
	}
	
	public WikipediaRecord(Text xml) throws IllegalArgumentException {
		this(xml.toString());
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public int getRevisionId() {
		return revisionId;
	}

	public void setRevisionId(int revisionId) {
		this.revisionId = revisionId;
	}

	public Date getRevisionDate() {
		return revisionDate;
	}

	public void setRevisionDate(Date revisionDate) {
		this.revisionDate = revisionDate;
	}

	public String getRevisionComment() {
		return revisionComment;
	}

	public void setRevisionComment(String revisionComment) {
		this.revisionComment = revisionComment;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

}
