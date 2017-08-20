package org.thebigjc.hackreduce;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class StockPairRecord {
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Date date;
	private StockClose close1;
	private StockClose close2;

	public StockPairRecord(Text value) {
		this(value.toString());
	}
	
	public StockPairRecord(String value) {
		this(value.split("\t"));
	}

	public StockPairRecord(String key, String value) {
		String keys[] = key.split("\\|");
		String ds[] = value.split("\\|");
		
		try {
			Date d = sdf.parse(keys[2]);
			this.setDate(d);
		} catch (ParseException e) {
			throw new RuntimeException(key, e);
		}
		
		this.setClose1(new StockClose(keys[0], Double.parseDouble(ds[0])));
		this.setClose2(new StockClose(keys[1], Double.parseDouble(ds[1])));
	}

	public StockPairRecord(String[] split) {
		this(split[0], split[1]);
	}

	public void setClose1(StockClose close1) {
		this.close1 = close1;
	}

	public StockClose getClose1() {
		return close1;
	}

	public void setClose2(StockClose close2) {
		this.close2 = close2;
	}

	public StockClose getClose2() {
		return close2;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Date getDate() {
		return date;
	}

	public String getDateString() {
		return sdf.format(getDate());
	}

	public String getKey() {
		return close1.getTicker() + "|" + close2.getTicker() + "|" + getDateString();
	}
	
	public String getValue() {
		return close1.getClose() + "|" + close2.getClose();
	}

	public String getPairKey() {
		return close1.getTicker() + "|" + close2.getTicker();
	}
	
	

}
