package mapreduceTest.phoneflow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PhoneFlowReducer extends Reducer<Text, PhoneFlow, Text, Text> {

	Text phoneDetail =new Text();
	@Override
	protected void reduce(Text phoneNum, Iterable<PhoneFlow> flowNumIterable, Context arg2)
			throws IOException, InterruptedException {

		int upFlowInt = 0;
		int downFlowInt = 0;
		int sumFlowInt = 0;
		for (PhoneFlow phoneFlow : flowNumIterable) {
			upFlowInt+=phoneFlow.getUpFlow();
			downFlowInt+=phoneFlow.getDownFlow();
			sumFlowInt+=phoneFlow.getSumFlow();
		}
		phoneDetail.set(upFlowInt+"  "+downFlowInt+"  "+sumFlowInt);
		arg2.write(phoneNum, phoneDetail);
	}
}
