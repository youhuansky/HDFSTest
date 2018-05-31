package mapreduceTest.priceSortTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PriceSortDriver {

	
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job instance = Job.getInstance(configuration);
		instance.setJarByClass(PriceSortDriver.class);
		instance.setMapperClass(PriceSortMapper.class);
		instance.setReducerClass(PriceSortReducer.class);
		instance.setMapOutputKeyClass(Product.class);
		instance.setMapOutputValueClass(Text.class);
		instance.setOutputKeyClass(Text.class);
		instance.setOutputValueClass(Product.class);
		instance.setPartitionerClass(ProdtSortPartitioner.class);
		instance.setGroupingComparatorClass(GroupingComparator.class);
		instance.setNumReduceTasks(3);
		FileInputFormat.setInputPaths(instance, args[0]);
		FileOutputFormat.setOutputPath(instance,new Path(args[1]));
		
		
		instance.waitForCompletion(true);
		
	}
}
