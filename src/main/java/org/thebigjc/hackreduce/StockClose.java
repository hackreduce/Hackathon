package org.thebigjc.hackreduce;

public class StockClose {
	private String ticker;
	private double close;
	public void setTicker(String ticker) {
		this.ticker = ticker;
	}
	public String getTicker() {
		return ticker;
	}
	public void setClose(double close) {
		this.close = close;
	}
	public double getClose() {
		return close;
	}
	public StockClose(String ticker, double close) {
		super();
		this.ticker = ticker;
		this.close = close;
	}
}
