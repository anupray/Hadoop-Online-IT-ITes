package com.training.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemOperations {
	
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path pth = new Path(args[0]);
		
		if (fs.exists(pth)) {
			System.out.println("Path Exists.. Now Deleting..");
			fs.delete(pth, true);
		}else {
			System.out.println("Path Doesn't Exist.. Now Creating..");
			fs.create(pth);
			System.out.println("Path Created : " + args[0]);
			
		}
		
	}

}
