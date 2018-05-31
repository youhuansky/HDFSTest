package mapreduceTest.combinerTest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SelfDefineCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Context arg2) throws IOException, InterruptedException {
		Long count=0L;
		for (LongWritable longWritable : arg1) {
			count=count+longWritable.get();
		}
		arg2.write(arg0, new LongWritable(count));
		
	}
}
