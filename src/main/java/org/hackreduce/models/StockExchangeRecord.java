package org.hackreduce.models;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;


/**
 * Parses a raw record (line of string data) from the NASDAQ/NYSE CSV data dump into a Java object.
 *
 * Data dump can be found at:
 * http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nasdaq-exchange
 * http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nyse-exchange
 *
 */
public class StockExchangeRecord {

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	String exchange;
	String stockSymbol;
	Date date;
	double stockPriceOpen;
	double stockPriceHigh;
	double stockPriceLow;
	double stockPriceClose;
	int stockVolume;
	double stockPriceAdjClose;

	public StockExchangeRecord(String inputString) throws IllegalArgumentException {
		// CSV header (parsing the inputString is based on this):
		// exchange, stock_symbol, date, stock_price_open, stock_price_high, stock_price_low,
		// 		stock_price_close, stock_volume, stock_price_adj_close
		String[] attributes = inputString.split(",");

		if (attributes.length != 9)
			throw new IllegalArgumentException("Input string given did not have 9 values in CSV format");

		try {
			setExchange(attributes[0]);
			setStockSymbol(attributes[1]);
			setDate(sdf.parse(attributes[2]));
			setStockPriceOpen(Double.parseDouble(attributes[3]));
			setStockPriceHigh(Double.parseDouble(attributes[4]));
			setStockPriceLow(Double.parseDouble(attributes[5]));
			setStockPriceClose(Double.parseDouble(attributes[6]));
			setStockVolume(Integer.parseInt(attributes[7]));
			setStockPriceAdjClose(Double.parseDouble(attributes[8]));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Input string contained an unknown value that couldn't be parsed", e);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
	}

	public StockExchangeRecord(Text inputText) throws IllegalArgumentException {
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

	public double getStockPriceOpen() {
		return stockPriceOpen;
	}

	public void setStockPriceOpen(double stockPriceOpen) {
		this.stockPriceOpen = stockPriceOpen;
	}

	public double getStockPriceHigh() {
		return stockPriceHigh;
	}

	public void setStockPriceHigh(double stockPriceHigh) {
		this.stockPriceHigh = stockPriceHigh;
	}

	public double getStockPriceLow() {
		return stockPriceLow;
	}

	public void setStockPriceLow(double stockPriceLow) {
		this.stockPriceLow = stockPriceLow;
	}

	public double getStockPriceClose() {
		return stockPriceClose;
	}

	public void setStockPriceClose(double stockPriceClose) {
		this.stockPriceClose = stockPriceClose;
	}

	public int getStockVolume() {
		return stockVolume;
	}

	public void setStockVolume(int stockVolume) {
		this.stockVolume = stockVolume;
	}

	public double getStockPriceAdjClose() {
		return stockPriceAdjClose;
	}

	public void setStockPriceAdjClose(double stockPriceAdjClose) {
		this.stockPriceAdjClose = stockPriceAdjClose;
	}

}
