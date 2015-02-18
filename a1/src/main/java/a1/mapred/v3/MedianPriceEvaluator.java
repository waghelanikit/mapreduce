package a1.mapred.v3;

import java.io.DataInput;
import java.io.DataOutput;
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
 * 
 * @author nikit
 *
 */
public class MedianPriceEvaluator {
	private static final SimpleLogger LOGGER = new SimpleLogger(MedianPriceEvaluator.class);
	/**
	 * Mapper generates the CustomKey and emits it as key and price as value
	 * @author nikit
	 * produces <CustomKey, DoubleWritable>
	 */
	static class MedianPriceMapper
	extends Mapper<LongWritable, Text, CustomKey, DoubleWritable> {
		Parser parser = new Parser();
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
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
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(CustomKey key, Iterable<DoubleWritable> values,
				Context context)
						throws IOException, InterruptedException {

			List<Double> list = new ArrayList<Double>();

			for(DoubleWritable val : values) {
				list.add(val.get());
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
	 * Partition class takes care of relaying sets of data to reducers
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
	 * Comparator takes care of sorting based on CustomKey
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
	 * GroupComparator takes care of sorting the final Keys form Mapper
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
	 * CustomKey to help sort the keys based on the subordinate value
	 * @author nikit
	 * key: Category
	 * value: price
	 *
	 */
	static class CustomKey implements WritableComparable<CustomKey>{
		private String key;
		private Double value;
		
		public void setKey(final String key) {
			this.key = key;
		}
		 
		public void setValue(final Double value) {
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

		public int compareTo(final CustomKey o) {
			// TODO Auto-generated method stub
			if(o == null)
				return 0;
			
			int result =  key.compareTo(o.key);
			if(result == 0)
				result = this.value.compareTo(o.value);
			return result;
		}
		
		@Override
		public String toString() {
			return key.toString();
		}
	}
	/**
	 * Main
	 * @param args
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

		job.setPartitionerClass(KeyPartitioners.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(CustomKey.class);
		
		job.setNumReduceTasks(60);
		
		FileUtils.deleteDirectory(new File(args[1]));
		Timer timer = new Timer().start();
		
		int exit = (job.waitForCompletion(true) ? 0 : 1);
		
		LOGGER.info(timer.stop());
		LOGGER.info("Exit"+exit);
		LOGGER.close();
	}
}