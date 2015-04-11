package com.airdel.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.airdel.model.AirlineEntry;
import com.airdel.util.Parser;

public class AirDelMapper extends
	Mapper<Object, Text, Text, AirlineEntry>{
	private static final String Carrier = "Carrier";
	private static final String FlightDate = "FlightDate";
	private static final int ONE = 1;
	Parser parser = new Parser(',');
	/**
	 * map function to read each line and emit <Text, AirlineEntry> 
	 * @author nikit
	 * produces <Text, AirlineEntry>
	 */
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		parser.parse(value.toString());
		
		// ignore if invalid data
		if (!parser.isValid()) {
			return;
		}

		try {
			
			AirlineEntry outVal = new AirlineEntry();
			outVal.setAname(parser.getText(Carrier));
			outVal.setFdate(parser.getText(FlightDate));
			outVal.setAirTimeDelay(parser.getInt("ActualElapsedTime") - 
					parser.getInt("CRSElapsedTime"));
			outVal.setDistance(parser.getInt("Distance"));
			outVal.setDepDelay(parser.getInt("DepDelay"));
			outVal.setArrDelay(parser.getInt("ArrDelay"));
			outVal.setDest(parser.getText("Dest"));
			outVal.setOrigin(parser.getText("Origin"));
			outVal.setFcode(parser.getText("TailNum"));
			outVal.setNum(ONE);
			// emit data
			context.write(new Text(parser.getText(Carrier)), outVal);
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return;
		}
	}
}