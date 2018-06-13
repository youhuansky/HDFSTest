package mapreduceTest.indextest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	
	
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		long count=0;
		for (LongWritable longWritable : arg1) {
			count+=1;
		}
		
		arg2.write(arg0, new LongWritable(count));
	}
}
