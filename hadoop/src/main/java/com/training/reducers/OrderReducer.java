package com.training.reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	public void reduce(Text key, Iterable<LongWritable> val, Context con) throws IOException, InterruptedException {
		
		long sum = 0;
		
		for (LongWritable l : val) {
			sum += l.get();
		}
		
		con.write(key, new LongWritable(sum));
		
	}

}
