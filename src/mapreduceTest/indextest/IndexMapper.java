package mapreduceTest.indextest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	String fileName;
	
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
		FileSplit fileSplit=(FileSplit)inputSplit;
		
		Path path = fileSplit.getPath();
		fileName = path.getName();
		
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String string = value.toString();
		String[] split = string.split(" ");
		
		
		for (String string2 : split) {
			context.write(new Text(string2+"--"+fileName), new LongWritable(1));
		}
	
	
	}

}
