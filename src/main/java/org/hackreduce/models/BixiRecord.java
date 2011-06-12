package org.hackreduce.models;

import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.Text;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class BixiRecord {

	Logger LOG = Logger.getLogger(BixiRecord.class.getName());
	
	private static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd_MM_yyyy__HH_mm_ss");

	private Date date;
	private int stationId;
	private String name;
	private String terminalName;
	private double latitude;
	private double longitude;
	private boolean installed;
	private boolean locked;
	private Date installDate;
	private Date removalDate;
	private boolean temporary;
	private int nbBikes;
	private int nbEmptyDocks;
	
	public BixiRecord(String xmlFilename, String xml) throws IllegalArgumentException {
		String filename = xmlFilename.endsWith(".xml") ? xmlFilename.replace(".xml", "") : xmlFilename;

		// XML is like violence...
	    try {
	    	DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	    	InputSource is = new InputSource();
	        is.setCharacterStream(new StringReader(xml));
			Document doc = docBuilder.parse(is);

			NodeList stationList = doc.getElementsByTagName("station");
			if (stationList.getLength() != 1)
				throw new IllegalArgumentException();

			Element station = (Element)stationList.item(0);

			setStationId(Integer.parseInt(getStringFromTag(station, "id")));
			setName(getStringFromTag(station, "name"));
			setTerminalName(getStringFromTag(station, "terminalName"));
			setLatitude(Double.parseDouble(getStringFromTag(station, "lat")));
			setLongitude(Double.parseDouble(getStringFromTag(station, "long")));
			setInstalled(Boolean.parseBoolean(getStringFromTag(station, "installed")));
			setLocked(Boolean.parseBoolean(getStringFromTag(station, "locked")));

			String installDateString = getStringFromTag(station, "installDate");
			if (installDateString != null)
				setInstallDate(new Date(Long.parseLong(installDateString)));

			String removalDateString = getStringFromTag(station, "removalDate");
			if (removalDateString != null)
				setRemovalDate(new Date(Long.parseLong(removalDateString)));

			String dateString = getStringFromTag(station, "date");
			if (dateString != null)
				setDate(new Date(Long.parseLong(dateString)));
			
			setTemporary(Boolean.parseBoolean(getStringFromTag(station, "temporary")));
			setNbBikes(Integer.parseInt(getStringFromTag(station, "nbBikes")));
			setNbEmptyDocks(Integer.parseInt(getStringFromTag(station, "nbEmptyDocks")));
	    } catch (Exception e) {
	    	// Gotta catch 'em all!
	    	LOG.log(Level.WARNING, e.getMessage(), e);
			throw new IllegalArgumentException("Couldn't create a " + getClass().getName() + " record from the given XML");
		}
	    
	    if (getDate() == null) {
			try {
				setDate(SIMPLE_DATE_FORMAT.parse(filename));
			} catch (ParseException e) {
				throw new IllegalArgumentException("Couldn't extract the date from the XML filename: " + xmlFilename);
			}
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
	
	public BixiRecord(Text xmlFilename, Text xml) throws IllegalArgumentException {
		this(xmlFilename.toString(), xml.toString());
	}


	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public int getStationId() {
		return stationId;
	}

	public void setStationId(int stationId) {
		this.stationId = stationId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTerminalName() {
		return terminalName;
	}

	public void setTerminalName(String terminalName) {
		this.terminalName = terminalName;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public boolean isInstalled() {
		return installed;
	}

	public void setInstalled(boolean installed) {
		this.installed = installed;
	}

	public boolean isLocked() {
		return locked;
	}

	public void setLocked(boolean locked) {
		this.locked = locked;
	}

	public Date getInstallDate() {
		return installDate;
	}

	public void setInstallDate(Date installDate) {
		this.installDate = installDate;
	}

	public Date getRemovalDate() {
		return removalDate;
	}

	public void setRemovalDate(Date removalDate) {
		this.removalDate = removalDate;
	}

	public boolean isTemporary() {
		return temporary;
	}

	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}

	public int getNbBikes() {
		return nbBikes;
	}

	public void setNbBikes(int nbBikes) {
		this.nbBikes = nbBikes;
	}

	public int getNbEmptyDocks() {
		return nbEmptyDocks;
	}

	public void setNbEmptyDocks(int nbEmptyDocks) {
		this.nbEmptyDocks = nbEmptyDocks;
	}
	
	public String toXML() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("<station>\n");
		buffer.append(createXMLTag(String.valueOf(getStationId()), "id"));
		buffer.append(createXMLTag(String.valueOf(getName()), "name"));
		buffer.append(createXMLTag(String.valueOf(getTerminalName()), "terminalName"));
		buffer.append(createXMLTag(String.valueOf(getLatitude()), "lat"));
		buffer.append(createXMLTag(String.valueOf(getLongitude()), "long"));
		buffer.append(createXMLTag(String.valueOf(getInstallDate()), "installed"));
		buffer.append(createXMLTag(String.valueOf(isLocked()), "locked"));
		buffer.append(createXMLTag(getInstallDate() != null ? String.valueOf(getInstallDate().getTime()) : null, "installDate"));
		buffer.append(createXMLTag(getRemovalDate() != null ? String.valueOf(getRemovalDate().getTime()) : null, "removalDate"));
		buffer.append(createXMLTag(String.valueOf(isTemporary()), "temporary"));
		buffer.append(createXMLTag(String.valueOf(getNbBikes()), "nbBikes"));
		buffer.append(createXMLTag(String.valueOf(getNbEmptyDocks()), "nbEmptyDocks"));
		buffer.append(createXMLTag(getDate() != null ? String.valueOf(getDate().getTime()) : null, "date"));
		buffer.append("</station>\n");
		return buffer.toString();
	}
	
	/**
	 * Create an XML tag for this Bixi record.
	 * @param value
	 * @param tagName
	 * @return
	 */
	public String createXMLTag(String value, String tagName) {
		StringBuffer buffer = new StringBuffer();
		buffer.append('<');
		buffer.append(tagName);
		if (value != null) {
			buffer.append('>');
			buffer.append(value);
			buffer.append("</");
			buffer.append(tagName);
		} else {
			buffer.append('/');
		}
		buffer.append(">\n");
		return buffer.toString();
	}

}
