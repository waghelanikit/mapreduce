package a1.mapred.v4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;

import mapred.utils.*;
import mapred.utils.Timer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/**
 * The Mapper performs sorting and passes on the data to reducer
 * in already sorted order
 * Uses:	KeyComparator
 * 		 	GroupingComparator
 * 			Partitioner
 * 			CustomKey
 * also evaluates fibonacci at every map call
 * @author nikit
 *
 */
public class MedianPriceEvaluator {
	// fibonacci limit
	static int N = 10;
	static final SimpleLogger LOGGER = new SimpleLogger(MedianPriceEvaluator.class);
	
	/**
	 * Mapper generates the CustomKey and emits it as key and price as value
	 * also generates fibonacci till N
	 * @author nikit
	 * produces <CustomKey, DoubleWritable>
	 */
	static class MedianPriceMapper
	extends Mapper<LongWritable, Text, CustomKey, DoubleWritable> {
		Parser parser = new Parser();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				parser.parse(value.toString());
				
				if(!parser.isValid()) {
					return;
				}
				CustomKey k = new CustomKey();
				k.setKey(parser.getCat());
				k.setValue(parser.getVal());
				context.write(k, new DoubleWritable(k.getValue()));
				fibonacci(N);
		}
		/**
		 * calculate fibonacci recursively
		 * @param n
		 * @return
		 */
		public int fibonacci(int n) {
			if(n == 1 || n == 2){
	            return 1;
	        }
	 
	        return fibonacci(n-1) + fibonacci(n-2);
		}
	}
	
	/**
	 * Reducer does not have to sort the data anymore
	 * It just find the median
	 * @author nikit
	 * Produces <Text, DoubleWritable>
	 */
	static class MedianPriceReducer
	extends Reducer<CustomKey, DoubleWritable, Text, DoubleWritable> {
		public void reduce(CustomKey key, Iterable<DoubleWritable> values,
				Context context)
						throws IOException, InterruptedException {

			List<Double> list = new ArrayList<Double>();
			Iterator<DoubleWritable> val = values.iterator();
			
			while(val.hasNext()) {
				list.add(val.next().get());
			}
			
			Double median;
			if(list.size() %2 != 0) {
				median = list.get(list.size()/2);
			} else {
				median = list.get(list.size()/2 - 1) + list.get(list.size()/2);
				median /= 2;
			}
			context.write(new Text(key.getKey()), new DoubleWritable(median));
		}
	}
	
	/**
	 * Partitioner class that takes care of relaying correct set of data to reducers
	 * @author nikit
	 *
	 */
	static class KeyPartitioners extends Partitioner<CustomKey, DoubleWritable> {
		HashPartitioner<String, DoubleWritable> hashPartitioner = new HashPartitioner<String, DoubleWritable>();
		
		@Override
		public int getPartition(CustomKey key, DoubleWritable value, int num) {
			// TODO Auto-generated method stub
			try {
				return hashPartitioner.getPartition(key.getKey(), value, num);
			} catch (Exception e) {
				e.printStackTrace();
				return (int) (Math.random() * num); 
			}
		}
	}
	/**
	 * Comparator for sorting with key and value both
	 * @author nikit
	 *
	 */
	static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(CustomKey.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CustomKey k1 = (CustomKey) w1;
			CustomKey k2 = (CustomKey) w2;
			int result = k1.getKey().compareTo(k2.getKey());
			if(result == 0) {
				result = k1.getValue().compareTo(k2.getValue());
			}
			return result;
		}
	}
	/**
	 * comparator for sort with keys (input to reducer)
	 * @author nikit
	 *
	 */
	static class GroupComparator extends WritableComparator{

		protected GroupComparator() {
			super(CustomKey.class, true);
			// TODO Auto-generated constructor stub
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CustomKey k1 = (CustomKey) w1;
			CustomKey k2 = (CustomKey) w2;
			return k1.getKey().compareTo(k2.getKey());
		}
	}
	
	/**
	 * Custom key used by mapper to sort the data
	 * key: Category
	 * value: price
	 * @author nikit
	 *
	 */
	static class CustomKey implements WritableComparable<CustomKey>{
		private String key;
		private Double value;
		
		public void setKey(String key) {
			this.key = key;
		}
		 
		public void setValue(Double value) {
			this.value = value;
		}
		
		public String getKey() {
			return key; 	
		} 
		
		public Double getValue() {
			return value;
		}
		

		public void readFields(DataInput in) throws IOException {
			this.key = WritableUtils.readString(in);
			this.value = in.readDouble();
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			WritableUtils.writeString(out, key.toString());
			out.writeDouble(value);
		}

		public int compareTo(CustomKey o) {
			// TODO Auto-generated method stub
			if(o == null)
				return 0;
			
			int result =  key.compareTo(o.getKey());
			if(result == 0)
				result = this.getValue().compareTo(o.getValue());
			return result;
		}
		
		@Override
		public String toString() {
			return key.toString();
		}
	}

	/**
	 * run method
	 * @param args
	 * @throws Exception
	 */
	public static void run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MedianPriceEvaluator <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(MedianPriceEvaluator.class);
		
		FileUtils.deleteDirectory(new File(args[1]));
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MedianPriceMapper.class);
		job.setReducerClass(MedianPriceReducer.class);

		job.setPartitionerClass(KeyPartitioners.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(CustomKey.class);
		
		job.setNumReduceTasks(5);
		
		Timer timer = new Timer().start();
		int exit = (job.waitForCompletion(true) ? 0 : 1);
		LOGGER.info(timer.stop());
		LOGGER.info("Exit"+exit);
		LOGGER.close();
	}
	/**
	 * Main
	 * @param args
	 * @throws Exception
	 */
	public static void main (String args[]) throws Exception {
		MedianPriceEvaluator.run(args);
	}
}