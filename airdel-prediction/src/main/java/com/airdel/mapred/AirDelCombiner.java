package com.airdel.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.airdel.model.AirlineEntry;


public class AirDelCombiner extends
	Reducer<Text, AirlineEntry, Text, AirlineEntry> {

	// map to store data temporarily
	Map<String, AirlineEntry> map = new HashMap<String, AirlineEntry>();
	
	/**
	 * reduces the flow of input to reducer
	 * produces <Text, AirlineEntry>
	 * @author nikit
	 */
	protected void reduce(Text key,
		Iterable<AirlineEntry> values, Context context)
		throws IOException, InterruptedException {
		// clear map before reuse
		map.clear();
		
		for (AirlineEntry v : values) {
			int delay = v.getAirTimeDelay();
			delay += Math.max(v.getDepDelay(), v.getArrDelay());
			
			if(delay > 0) {
				AirlineEntry av = map.get(v.getFcode());
				if(av != null) {
					av.setNum(av.getNum() + v.getNum());
				} else {
					map.put(v.getFcode(), v.clone());
				}
			}
		}
		
		// emit AirlineEntry per each entry in map
		for(String fcode: map.keySet()) {
			context.write(key, map.get(fcode));
		}
	}
}