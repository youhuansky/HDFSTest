package mapreduceTest.flowSort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSortReducer extends Reducer<PhoneFlow, Text, Text, PhoneFlow> {

	
	@Override
	protected void reduce(PhoneFlow arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		arg2.write(arg1.iterator().next(), arg0);
		
	}
}
