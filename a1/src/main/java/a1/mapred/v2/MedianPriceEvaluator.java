package a1.mapred.v2;

import java.io.File;
import java.io.IOException;
import java.util.*;

import mapred.utils.Parser;
import mapred.utils.SimpleLogger;
import mapred.utils.Timer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Median Evaluator where Reducer does the sorting.
 * @author nikit
 *
 */
public class MedianPriceEvaluator {
	private static final SimpleLogger LOGGER = new SimpleLogger(MedianPriceEvaluator.class);
	/**
	 * Median Mapper parses each line and emits its
	 * category as String key and price as double value.
	 * 
	 * produces <Text, DoubleWritable>
	 * @author nikit
	 *
	 */
	static class MedianPriceMapper
	extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		Parser parser = new Parser();
		
		/** Produces the key as category and value as price
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value.toString());
			if(parser.isValid()) {
				try {
					context.write(new Text(parser.getCat()), new DoubleWritable(parser.getVal()));
				} catch(NumberFormatException e) {}
			}
		}
	}
	/**
	 * Reducer sorts the list of Double values and determines median
	 * @author nikit
	 * produces <Text, DoubleWritable>
	 */
	static class MedianPriceReducer
	extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		/**
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context)
						throws IOException, InterruptedException {

			List<Double> list = new ArrayList<Double>();
			Iterator<DoubleWritable> val = values.iterator();
			
			while(val.hasNext()) {
				list.add(val.next().get());
			}
			// sorting here
			Collections.sort(list);
			
			Double median;
			if(list.size() %2 != 0) {
				median = list.get(list.size()/2);
			} else {
				median = list.get(list.size()/2 - 1) + list.get(list.size()/2);
				median /= 2;
			}
			context.write(key, new DoubleWritable(median));
		}
	}
	
	/**
	 * Main method executes the job and evaluates time of execution
	 * @param args <input path> <output path>
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MedianPriceEvaluator <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(MedianPriceEvaluator.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MedianPriceMapper.class);
		job.setReducerClass(MedianPriceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(60);
		
		FileUtils.deleteDirectory(new File(args[1]));
		Timer timer = new Timer().start();
		int exit = (job.waitForCompletion(true) ? 0 : 1);
		
		LOGGER.info(timer.stop());
		LOGGER.info("Exit"+exit);
		LOGGER.close();
	}
}