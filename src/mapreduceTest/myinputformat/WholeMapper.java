package mapreduceTest.myinputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class WholeMapper extends Mapper<NullWritable,BytesWritable, Text, BytesWritable> {

	
	@Override
	protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit();
	}
	
	@Override
	protected void map(NullWritable key, BytesWritable value,
			Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}
}
