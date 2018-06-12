package mapreduceTest.myinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WholeDriver {

	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WholeDriver.class);
		job.setMapperClass(WholeMapper.class);
		job.setReducerClass(WholeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(WholeInputFormat.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
		
		
	}
}
