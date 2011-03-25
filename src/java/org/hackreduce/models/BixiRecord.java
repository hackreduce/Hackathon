package org.hackreduce.models;

import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

	SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy__HH_mm_ss");

	Date date;
	int stationId;
	String name;
	String terminalName;
	double latitude;
	double longitude;
	boolean installed;
	boolean locked;
	Date installDate = null;
	Date removalDate = null;
	boolean temporary;
	int nbBikes;
	int nbEmptyDocks;

	public BixiRecord(String xmlFilename, String xml) throws IllegalArgumentException {
		String filename = xmlFilename.endsWith(".xml") ? xmlFilename.replace(".xml", "") : xmlFilename;
		try {
			setDate(sdf.parse(filename));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Couldn't extract the date from the XML filename: " + xmlFilename);
		}

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

			setTemporary(Boolean.parseBoolean(getStringFromTag(station, "temporary")));
			setNbBikes(Integer.parseInt(getStringFromTag(station, "nbBikes")));
			setNbEmptyDocks(Integer.parseInt(getStringFromTag(station, "nbEmptyDocks")));
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

}
