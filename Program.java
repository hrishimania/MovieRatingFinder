package edu.tamu.isys.ratings;

import java.io.IOException;
/*
 * This program implements Map-Reduce based application to extract movies 
 * with highest average rating for each genre in the input file.
 * Steps to run program are as follows:
 * Step 1 - Place 524002653.jar file at C:\hadoop\share\hadoop\mapreduce directory.
 * Step 2 - Format namenode and start HDFS(Namenode,Datanode, Node Manager Resource Manager).
 * Step 3 - Make /input directory in file system.
 * Step 4 - Copy input file from local disk to file syatem at directory /input.
 * Step 5 - Execute Program.jar.
 *    
 * @author Hrishikesh Shirsikar
 * @version 1.0
 * @Date: 11/3/2015
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.tamu.isys.ratings.RatingMapper;
import edu.tamu.isys.ratings.RatingReducer;

public class Program {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		try
		{
			Job job = Job.getInstance(conf, "RatingFinder");

			job.setMapperClass(RatingMapper.class);
			job.setCombinerClass(RatingCombiner.class);
			job.setReducerClass(RatingReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
			
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}
}
