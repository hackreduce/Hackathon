package org.hackreduce.models;

import java.util.Date;

import org.apache.hadoop.io.Text;


/**
 * Parses a raw record (line of string data) from the Flight CSV data dump into a Java object.
 *
 */
public class FlightRecord {

	String origin;
	String destination;
	Date departureTime;
	Date returnTime;
	double price;
	Date date;


	public FlightRecord(String inputString) throws IllegalArgumentException {
		// CSV legend (parsing the inputString is based on this):
		// origin, destination, departure time, return time, price, timestamp of the reading
		String[] attributes = inputString.split(",");

		if (attributes.length != 6)
			throw new IllegalArgumentException("Input string given did not have 6 values in CSV format");

		try {
			setOrigin(attributes[0]);
			setDestination(attributes[1]);
			setDepartureTime(new Date(Long.parseLong(attributes[2])));
			setReturnTime(new Date(Long.parseLong(attributes[3])));
			setPrice(Double.parseDouble(attributes[4]));
			setDate(new Date(Long.parseLong(attributes[5])));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
	}

	public FlightRecord(Text inputText) throws IllegalArgumentException {
		this(inputText.toString());
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public Date getDepartureTime() {
		return departureTime;
	}

	public void setDepartureTime(Date departureTime) {
		this.departureTime = departureTime;
	}

	public Date getReturnTime() {
		return returnTime;
	}

	public void setReturnTime(Date returnTime) {
		this.returnTime = returnTime;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

}
