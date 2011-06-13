package org.hackreduce.models;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;


/**
 * Parses a raw dividend record (line of string data) from the NASDAQ/NYSE CSV data dump into a Java object.
 *
 * Data dump can be found at:
 * http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nasdaq-exchange
 * http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nyse-exchange
 *
 */
public class StockExchangeDividend {

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	String exchange;
	String stockSymbol;
	Date date;
	double dividend;

	public StockExchangeDividend(String inputString) throws IllegalArgumentException {
		// CSV header (parsing the inputString is based on this):
		// exchange,stock_symbol,date,dividends
		String[] attributes = inputString.split(",");

		if (attributes.length != 4)
			throw new IllegalArgumentException("Input string given did not have 9 values in CSV format");

		try {
			setExchange(attributes[0]);
			setStockSymbol(attributes[1]);
			setDate(sdf.parse(attributes[2]));
			setDividend(Double.parseDouble(attributes[3]));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Input string contained an unknown value that couldn't be parsed", e);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
	}

	public StockExchangeDividend(Text inputText) throws IllegalArgumentException {
		this(inputText.toString());
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getStockSymbol() {
		return stockSymbol;
	}

	public void setStockSymbol(String stockSymbol) {
		this.stockSymbol = stockSymbol;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public double getDividend() {
		return dividend;
	}

	public void setDividend(double dividend) {
		this.dividend = dividend;
	}

}
