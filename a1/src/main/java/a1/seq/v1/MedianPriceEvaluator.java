package a1.seq.v1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import mapred.utils.Parser;
import mapred.utils.SimpleLogger;
import mapred.utils.Timer;

/**
 * Median Evaluator sequential version
 * @author nikit
 *
 */
public class MedianPriceEvaluator {
	private static final SimpleLogger LOGGER = new SimpleLogger(MedianPriceEvaluator.class);
	private PrintStream out;
	private String[] args;
	/**
	 * Main
	 * @param args
	 * @throws IOException
	 */
	public static void main (String args[]) throws IOException {
		Timer timer = new Timer().start();
		int exit = new MedianPriceEvaluator(args).run();
		
		LOGGER.info(timer.stop());
		LOGGER.info("Exit"+exit);
		LOGGER.close();
	}
	
	/**
	 * Constructor creates output stream
	 * @param args
	 * @throws IOException
	 */
	public MedianPriceEvaluator(String args[]) throws IOException {
		this.args = args;
		if(args.length < 2) {
			usage();
			System.exit(200);
		}
		try {
			File f = new File(args[1]);
			if(!f.isDirectory())
				f.mkdirs();
			out = new PrintStream(new FileOutputStream(args[1]+"/outv1"));
		} catch(Exception e) {
			e.printStackTrace();
			out = System.out;
		}
	}
	
	/**
	 * run
	 * @return exit code
	 * @throws IOException
	 */
	public int run () throws IOException {
		
		parser(args[0]);
		out.close();
		return 0;
	}
	
	/**
	 * prints usage
	 */
	public void usage() {
		System.out.print("java MedianPriceEvaluator <input file location> <output file directory>");
	}
	
	/**
	 * parses each line and puts the data in a map and sorts each entry
	 * @param path
	 * @throws IOException
	 */
	public void parser(String path) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		Map<String, List<Double>> priceMap = new HashMap<String,List<Double>>();
		Parser parser = new Parser();
		
		while((line = br.readLine()) != null) {
			parser.parse(line);
			if(!parser.isValid()) continue; 
			if(!priceMap.containsKey(parser.getCat())) 
				priceMap.put(parser.getCat(), new ArrayList<Double>());
			try {
				priceMap.get(parser.getCat()).add(parser.getVal());
			} catch(NumberFormatException e) {}
		}
		
		LOGGER.info("Map finished");
		for(String cat: priceMap.keySet()) {
			printMedian(cat, priceMap.get(cat));
		}
		LOGGER.info("Reduce finished");
		br.close();
	}
	
	/**
	 * prints the median
	 * @param key
	 * @param list
	 */
	public void printMedian(String key, List<Double> list) {
		if(list.size() == 0) return;
		Collections.sort(list);
		
		if(list.size() %2 != 0) {
			out.println(key+" "+list.get(list.size()/2));
		} else {
			double median = list.get(list.size()/2 - 1) + list.get(list.size()/2);
			median /= 2;
			out.println(key+" "+median);
		}
	}
}
