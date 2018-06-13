package mapreduceTest.indextest2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexTwoReducer extends Reducer<Text, Text, Text, Text> {

	
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		for (Text text : arg1) {
			sb.append(text.toString()+"\t");
		}
		
		arg2.write(arg0, new Text(sb.toString()));
	}
}
