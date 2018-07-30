package com.training.drivers;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.training.mappers.OrderMapper;
import com.training.reducers.OrderReducer;

public class OrderDriver {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		FileSystem fs = FileSystem.get(conf);

		Path ipath = new Path(args[0]);
		Path opath = new Path(args[1]);

		if (fs.exists(opath)) {
			fs.delete(opath, true);
		}

		job.setJarByClass(OrderDriver.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, ipath);
		FileOutputFormat.setOutputPath(job, opath);

		int result;
		try {
			result = job.waitForCompletion(true) ? 0 : 1;
			if (result == 0) {
				System.out.println("==> Success");
			} else {
				System.out.println("==> Failed");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
