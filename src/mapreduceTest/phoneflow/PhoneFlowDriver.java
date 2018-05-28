package mapreduceTest.phoneflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneFlowDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		Job instance = Job.getInstance(configuration);
		//设置驱动类
		instance.setJarByClass(PhoneFlowDriver.class);
		
		instance.setMapperClass(PhoneFlowMapper.class);
//		instance.setReducerClass(PhoneFlowReducer.class);
		instance.setReducerClass(PhoneFlowBeanReducer.class);
		instance.setMapOutputKeyClass(Text.class);
		instance.setMapOutputValueClass(PhoneFlow.class);
		instance.setOutputKeyClass(Text.class);
		instance.setOutputValueClass(Text.class);
		instance.setPartitionerClass(PhonePartitioner.class);
		instance.setNumReduceTasks(1);
		
		
		FileInputFormat.setInputPaths(instance, new Path(args[0]));
		FileOutputFormat.setOutputPath(instance, new Path(args[1]));
		boolean waitForCompletion = instance.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
		
	}
}
