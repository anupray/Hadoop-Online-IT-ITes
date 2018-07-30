package com.training.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
	
	public void map(LongWritable key, Text val, Context con) throws IOException, InterruptedException {
		
		String rec = key.toString();
		String[] recArray = rec.split(",");
		String k = recArray[recArray.length - 1];
		
		con.write(new Text(k), new LongWritable(1));
		
	}
}
