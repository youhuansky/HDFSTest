package mapreduceTest.indextest2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		String[] split = string.split("--");
		
		context.write(new Text(split[0]), new Text(split[1]));
	
	}
	
	
}
