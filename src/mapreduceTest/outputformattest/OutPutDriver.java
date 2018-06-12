package mapreduceTest.outputformattest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OutPutDriver {
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(OutPutDriver.class);
		job.setMapperClass(OutPutMapper.class);
		job.setReducerClass(OutPutReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setOutputFormatClass(MyOutPutFormat.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		//指定success文件输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
		
		
	}
}
