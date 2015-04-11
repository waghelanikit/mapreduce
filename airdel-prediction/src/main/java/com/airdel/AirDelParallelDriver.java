package com.airdel;

import java.io.File;

import mapred.utils.Timer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.airdel.mapred.AirDelCombiner;
import com.airdel.mapred.AirDelMapper;
import com.airdel.mapred.AirDelReducer;
import com.airdel.model.AirlineEntry;

/**
 * 
 * @author nikit
 *
 */
public class AirDelParallelDriver {
	/**
	 * runs the Job and evaluates time taken by the process to execute
	 * @param args <input file> <output directory> [sample rate:0.0833]
	 * @throws Exception
	 */
	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length < 2) {
			System.err.println("Usage: AirDelParallelDriver "
					+ "<input file> <output directory>");
			System.exit(2);
		}
		
		Job job = new Job(conf,
				"Approximate Median Evaluator");
		
		job.setJarByClass(AirDelParallelDriver.class);
		
		job.setMapperClass(AirDelMapper.class);
		job.setCombinerClass(AirDelCombiner.class);
		job.setReducerClass(AirDelReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AirlineEntry.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AirlineEntry.class);
		
		job.setNumReduceTasks(60);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileUtils.deleteDirectory(new File(args[1]));
		
		Timer timer = new Timer();
		timer.start();
		int exit = job.waitForCompletion(true)? 0 : 1;
		System.out.println(timer.stop());
		System.exit(exit);
	}
	
	public static void main(String args[]) throws Exception {
		run(args);
	}
}
