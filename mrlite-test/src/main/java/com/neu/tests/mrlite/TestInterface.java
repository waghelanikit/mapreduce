package com.neu.tests.mrlite;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.List;

import com.neu.mrlite.*;

import static com.neu.mrlite.Assortment.*;

public class TestInterface {
	public static void run(String inFile, String outDir) throws FileNotFoundException, UnsupportedEncodingException, UnknownHostException {
		
		IOHandle io = new IOHandle(inFile, outDir);
		
		Assortment<String> col1 = readInputFrom(io);
		
		Assortment<Pair<String, Integer>> col2 = col1.parallel(
			new POCallback() {
				public void process(Writable line) {
					String[] parts = line.cast(String.class).split("\\t");
					emit(parts[0], Integer.parseInt(parts[1]));
				}
			});
		
		Assortment<Pair<String, Integer>> col3 = col2.parallel(
			new POCallback() {
				public void process(Writable obj) {
					int val = (Integer) obj.cast(Pair.class).value;
					String key = (String) obj.cast(Pair.class).key;
					emit(key, val+10);
				}
			});
		
		Assortment<Pair<String, Integer>> col4 = col3.parallel(
			new POCallback() {
				public void process(Writable obj) {
					int val = (Integer) obj.cast(Pair.class).value;
					String key = (String) obj.cast(Pair.class).key;
					emit(key, val+10);
				}
			});
	}
}