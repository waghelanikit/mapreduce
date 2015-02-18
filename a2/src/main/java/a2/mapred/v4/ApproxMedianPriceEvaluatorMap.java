package a2.mapred.v4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import mapred.utils.Parser;
import mapred.utils.SimpleLogger;
import mapred.utils.Timer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ApproxMedianPriceEvaluatorMap {
	private static SimpleLogger LOGGER = 
			new SimpleLogger(ApproxMedianPriceEvaluatorMap.class);
	static int i = 0;

	public static class ApproxMedianEvaluatorMapper extends
			Mapper<Object, Text, Text, MapWritable> {
		
		private static final LongWritable ONE = new LongWritable(1);
		
		Parser parsed = new Parser();
				

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if(i++ % 10 == 0) return;
			parsed.parse(value.toString());
			
			if (!parsed.isValid()) {
				return;
			}

			try {
				
				MapWritable outVal = new MapWritable();
				outVal.put(new DoubleWritable(parsed.getVal()), ONE);

				// write out the user ID with min max dates and count
				context.write(new Text(parsed.getCat()), outVal);

			} catch (Exception e) {
				System.err.println(e.getMessage());
				return;
			}
		}
	}

	public static class ApproxMedianEvaluatorCombiner
			extends
			Reducer<Text, MapWritable, Text, MapWritable> {

		
		protected void reduce(Text key,
				Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			MapWritable outValue = new MapWritable();
			
			for (MapWritable v : values) {
				for (Entry<Writable, Writable> entry : v.entrySet()) {
					
					DoubleWritable pkey = (DoubleWritable) entry.getKey();
					LongWritable count = (LongWritable) outValue.get(pkey);
					
					if (count != null) {
						count.set(count.get()
								+ ((LongWritable) entry.getValue()).get());
					} else {
						outValue.put(pkey, new LongWritable(
								((LongWritable) entry.getValue()).get()));
					}
					

				}
				
			}
			context.write(key, outValue);
		}
	}

	public static class ApproxMedianEvaluatorReducer
			extends
			Reducer<Text, MapWritable, Text, CustomData> {
		private CustomData result = new CustomData();
		private TreeMap<Double, Long> priceCounts = new TreeMap<Double, Long>();

		@Override
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			long total = 0;
			priceCounts.clear();
			result.setMedian(0.0);
			
			for (MapWritable v : values) {
				for (Entry<Writable, Writable> entry : v.entrySet()) {
					Double price = ((DoubleWritable) entry.getKey()).get();
					long count = ((LongWritable) entry.getValue()).get();

					total += count;

					Long storedCount = priceCounts.get(price);
					if (storedCount == null) {
						priceCounts.put(price, count);
					} else {
						priceCounts.put(price, storedCount + count);
					}
				}
			}

			long medianIndex = total / 2L;
			long previousPrices = 0;
			long prices = 0;
			double prevKey = 0.0;
			for (Entry<Double, Long> entry : priceCounts.entrySet()) {
				prices = previousPrices + entry.getValue();
				if (previousPrices <= medianIndex && medianIndex < prices) {
					if (total % 2 == 0) {
						if (previousPrices == medianIndex) {
							result.setMedian((Double) (entry.getKey() + prevKey) / 2.0);
						} else {
							result.setMedian(entry.getKey());
						}
					} else {
						result.setMedian(entry.getKey());
					}
					break;
				}
				previousPrices = prices;
				prevKey = entry.getKey();
			}

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length != 2) {
			System.err.println("Usage: ApproxMedianPriceEvaluator "
					+ "<input file> <output directory>");
			System.exit(2);
		}
		Job job = new Job(conf,
				"Approximate Median Evaluator");
		
		job.setJarByClass(ApproxMedianPriceEvaluatorMap.class);
		
		job.setMapperClass(ApproxMedianEvaluatorMapper.class);
		job.setCombinerClass(ApproxMedianEvaluatorCombiner.class);
		job.setReducerClass(ApproxMedianEvaluatorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
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
		LOGGER.close();
		System.exit(exit);
	}

	public static class CustomData implements Writable {
		private Double median = 0.0;

		public Double getMedian() {
			return median;
		}

		public void setMedian(Double median) {
			this.median = median;
		}

		public void readFields(DataInput in) throws IOException {
			median = in.readDouble();
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(median);
		}

		@Override
		public String toString() {
			return median.toString();
		}
	}
}