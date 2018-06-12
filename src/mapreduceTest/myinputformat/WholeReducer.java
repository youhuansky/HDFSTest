package mapreduceTest.myinputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WholeReducer extends Reducer<Text, BytesWritable, Text, Text> {

	
	@Override
	protected void reduce(Text arg0, Iterable<BytesWritable> arg1,
			Reducer<Text, BytesWritable, Text, Text>.Context arg2) throws IOException, InterruptedException {
		
		for (BytesWritable bytesWritable : arg1) {
			byte[] bytes = bytesWritable.getBytes();
			String string = new String(bytes);
			
			arg2.write(arg0, new Text(string));
		}
	}
}
