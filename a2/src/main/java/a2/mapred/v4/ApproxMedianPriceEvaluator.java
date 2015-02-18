package a2.mapred.v4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import mapred.utils.Parser;
import mapred.utils.SimpleLogger;
import mapred.utils.Timer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * This program generates Approximate medians using combiner between
 * mapper and reducer.
 * 
 * Goal is to minimize computation time by reducing number of inputs
 * to the reducer.
 * 
 * @author nikit
 *
 */
public class ApproxMedianPriceEvaluator {
	// Logger for exceptions and timer output 
	private static SimpleLogger LOGGER = 
			new SimpleLogger(ApproxMedianPriceEvaluator.class);
	
	// file line iterator
	static long i = 0;
	
	// Sample Rate to skip the data at particular rate 
	// [ eg : at every 12th line ]
	static float SR = 0.0833f;
	
	/**
	 * Approximate Median Mapper parses each line and emits its
	 * category as key and CustomData as value.
	 * 
	 * CustomData stores Key as Price and Value initialized to 1
	 * @author nikit
	 *
	 */
	public static class ApproxMedianEvaluatorMapper extends
			Mapper<Object, Text, Text, CustomData> {
		
		private static final Long ONE = new Long(1);
		
		Parser parsed = new Parser();
				
		/**
		 * map function to read each line and emit <Text, CustomData> 
		 * @author nikit
		 * {@inheritDoc}
		 * produces <Text, CustomData>
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// skip at Sample Rate
			if(i++ % (int)(1/SR) == 0) return;
			parsed.parse(value.toString());
			
			// ignore if invalid data
			if (!parsed.isValid()) {
				return;
			}

			try {
				
				CustomData outVal = new CustomData();
				outVal.setPrice(parsed.getVal());
				outVal.setNum(ONE);
				
				// emit data
				context.write(new Text(parsed.getCat()), outVal);

			} catch (Exception e) {
				System.err.println(e.getMessage());
				return;
			}
		}
	}

	/**
	 * Combiner takes same inputs as Reducer and produces same Output
	 * as Mapper. It sums up values of the repeated key Prices in 
	 * received Iterator and emits it to the reducer
	 * 
	 * @author nikit
	 *
	 */
	public static class ApproxMedianEvaluatorCombiner
			extends
			Reducer<Text, CustomData, Text, CustomData> {

		// map to store data temporarily
		Map<Double, Long> map = new HashMap<Double, Long>();
		
		/**
		 * reduces the flow of input to reducer
		 * produces <Text, CustomData>
		 * @author nikit
		 */
		protected void reduce(Text key,
				Iterable<CustomData> values, Context context)
				throws IOException, InterruptedException {
			// clear map before reuse
			map.clear();
			
			// merge the counts of each key price into a map
			for (CustomData v : values) {
				
					Double pkey = v.getPrice();
					Long count = (Long) map.get(pkey);
					
					if (count != null) {
						count +=  v.getNum();
					} else {
						map.put(pkey, v.getNum());
					}
			}
			
			// emit CustomData per each entry in map
			for(Double price: map.keySet()) {
				CustomData cd = new CustomData();
				cd.setPrice(price);
				cd.setNum(map.get(price));
				context.write(key, cd);
			}
		}
	}

	/**
	 * Reducer calculates an approximate median on the basis of
	 * counts of each CustumData received in Iterator
	 * 
	 * It emits the median value for each category of item.
	 * @author nikit
	 * 
	 */
	public static class ApproxMedianEvaluatorReducer
			extends
			Reducer<Text, CustomData, Text, DoubleWritable> {
		// reusable map for evaluating counts of each Key-Value CustomData
		private TreeMap<Double, Long> priceCounts = new TreeMap<Double, Long>();

		/**
		 * Evaluates median with approximation
		 * @author nikit
		 * 
		 * produces <Text, DoubleWritable>
		 */
		@Override
		public void reduce(Text key, Iterable<CustomData> values,
				Context context) throws IOException, InterruptedException {

			long total = 0;
			priceCounts.clear();
			
			// re-evaluate the counts and put them in a map for each key price
			for (CustomData v : values) {
					Double price = v.getPrice();
					long count = v.getNum();

					total += count;

					Long storedCount = priceCounts.get(price);
					if (storedCount == null) {
						priceCounts.put(price, count);
					} else {
						priceCounts.put(price, storedCount + count);
					}
			}

			// approximate  estimation of median
			long medianIndex = total / 2L;
			long previousPrices = 0;
			long prices = 0;
			double prevKey = 0.0;
			double median = 0.0;
			for (Entry<Double, Long> entry : priceCounts.entrySet()) {
				prices = previousPrices + entry.getValue();
				// median window
				if (previousPrices <= medianIndex && medianIndex < prices) {
					if (total % 2 == 0) {
						if (previousPrices == medianIndex) {
							median = (Double) (entry.getKey() + prevKey) / 2.0;
						} else {
							median  = entry.getKey();
						}
					} else {
						median = entry.getKey();
					}
					break;
				}
				previousPrices = prices;
				prevKey = entry.getKey();
			}
			
			context.write(key, new DoubleWritable(median));
		}
	}

	/**
	 * runs the Job and evaluates time taken by the process to execute
	 * @param args <input file> <output directory> [sample rate:0.0833]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length < 2) {
			System.err.println("Usage: ApproxMedianPriceEvaluator "
					+ "<input file> <output directory> [sample rate:0.0833]");
			System.exit(2);
		}
		
		if(args.length == 3) {
			try {
				SR = Float.parseFloat(args[2]);
			} catch (Exception e) {
				// do nothing
			}
		}
		
		Job job = new Job(conf,
				"Approximate Median Evaluator");
		
		job.setJarByClass(ApproxMedianPriceEvaluator.class);
		
		job.setMapperClass(ApproxMedianEvaluatorMapper.class);
		job.setCombinerClass(ApproxMedianEvaluatorCombiner.class);
		job.setReducerClass(ApproxMedianEvaluatorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CustomData.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CustomData.class);
		
		job.setNumReduceTasks(60);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileUtils.deleteDirectory(new File(args[1]));
		
		Timer timer = new Timer();
		timer.start();
		int exit = job.waitForCompletion(true)? 0 : 1;
		LOGGER.info(timer.stop());
		LOGGER.info("Exit"+exit+" lines(parsed):"+i);
		LOGGER.close();
		System.exit(exit);
	}

	/**
	 * CustomData that stores key and value as Price and Counter
	 * price: Double
	 * num: Long
	 * @author nikit
	 * Writable to map and reduce emit.
	 */
	public static class CustomData implements Writable {
		private Double price = 0.0;
		private Long num = 1L;
		
		// getters and setters
		
		public Double getPrice() {
			return price;
		}

		public void setPrice(Double price) {
			this.price = price;
		}
		
		public Long getNum() {
			return num;
		}
		
		public void setNum(long num) {
			this.num = num;
		}

		// Overridden from Writable
		
		public void readFields(DataInput in) throws IOException {
			price = in.readDouble();
			num = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(price);
			out.writeLong(num);
		}

		// Overridden from Object
		/**
		 * produces string representation of CustomData
		 * @author nikit
		 * return String 
		 * if price = 100.0 return "100.0" ignores num count
		 */
		@Override
		public String toString() {
			return price.toString();
		}
	}
}