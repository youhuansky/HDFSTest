package mapreduceTest.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OrderDriver {
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(OrderDriver.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
