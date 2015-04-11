package com.airdel.mapred;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.airdel.model.AirlineEntry;


public class AirDelReducer 
extends Reducer<Text, AirlineEntry, Text, Text> {
	
	TreeMap<String, AirlineEntry> aCounts = new TreeMap<String, AirlineEntry>();
	/**
	 * Evaluates median with approximation
	 * @author nikit
	 * 
	 * produces <Text, DoubleWritable>
	 */
	@Override
	public void reduce(Text key, Iterable<AirlineEntry> values,
		Context context) throws IOException, InterruptedException {
		long total = 0;
		int count = 0;
		aCounts.clear();
		
		// re-evaluate the counts and put them in a map for each key price
		for (AirlineEntry v : values) {
			int delay = v.getAirTimeDelay();
			delay += Math.max(v.getDepDelay(), v.getArrDelay());
			total += delay;
			count ++;
			if(delay > 0) {
				AirlineEntry av = aCounts.get(v.getFcode());
				if(av != null) {
					av.setNum(av.getNum() + v.getNum());
				} else {
					aCounts.put(v.getFcode(), v.clone());
				}
			}
		}

		long avg = total / count;
		
		for (String fcode: aCounts.keySet()) {
			AirlineEntry av = aCounts.get(fcode);
			int delay = av.getAirTimeDelay();
			delay += Math.max(av.getDepDelay(),av.getArrDelay());
			if((float)delay/avg >= 0.3f)
				context.write(key, new Text(fcode+","+av.getOrigin()+","+
					av.getDest()+","+av.getDistance()));
		}
	}
}
