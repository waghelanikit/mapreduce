package com.airdel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.airdel.util.Parser;


public class AirDelPredictor {
	private Parser parser;
	private static char DEL = ',';
	
	public AirDelPredictor() {
		parser = new Parser(DEL);
	}
	
	public static void main(String args[]) throws IOException {
		
	}
	
	/**
	 * Test for parser
	 * @param filename
	 */
	public void testParser(String filename) {
		if (parser == null) return;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			//String line = null;
			parser.parse(line);
			if(parser.isValid()){
				System.out.println(parser);
			} else {
				System.out.println("nothing to parse");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(br != null) {
				try {
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}