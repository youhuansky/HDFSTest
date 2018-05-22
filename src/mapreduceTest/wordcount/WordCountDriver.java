package mapreduceTest.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
//		System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.2");
		// 1.获取job对象
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		// 2.加载驱动类
		job.setJarByClass(WordCountDriver.class);
		// 3.加载mapper和reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		// 4.两个输出 map端输出的key和value类型，最终输出的key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 5.设置文件输出和输入的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6.提交
		// job.submit();
		boolean waitForCompletion = job.waitForCompletion(true);

		System.exit(waitForCompletion ? 0 : 1);
	}
}
