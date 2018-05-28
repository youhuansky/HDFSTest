package mapreduceTest.flowSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSortDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job instance = Job.getInstance(conf);
		
		
		instance.setJarByClass(FlowSortDriver.class);
		instance.setMapperClass(FlowSortMapper.class);
		instance.setReducerClass(FlowSortReducer.class);
		
		instance.setMapOutputKeyClass(PhoneFlow.class);
		instance.setMapOutputValueClass(Text.class);
		instance.setOutputKeyClass(Text.class);
		instance.setOutputValueClass(PhoneFlow.class);
		
		instance.setPartitionerClass(PhoneSortPartioner.class);
		instance.setNumReduceTasks(3);
		FileInputFormat.setInputPaths(instance, args[0]);
		FileOutputFormat.setOutputPath(instance,new Path( args[1]));
		
		boolean waitForCompletion = instance.waitForCompletion(true);
		
	}
}
