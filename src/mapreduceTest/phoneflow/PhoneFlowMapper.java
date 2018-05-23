package mapreduceTest.phoneflow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PhoneFlowMapper extends Mapper<LongWritable, Text, Text, PhoneFlow> {
	
	Text phoneNum=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String phoneInfo = value.toString();
		String[] split = phoneInfo.split("\\ ");
		PhoneFlow phoneFlow = new PhoneFlow();
		int upFlowInt = Integer.parseInt(split[7]);
		int downFlowInt = Integer.parseInt(split[8]);
		phoneFlow.setUpFlow(upFlowInt);
		phoneFlow.setDownFlow(downFlowInt);
		phoneFlow.setSumFlow(upFlowInt+downFlowInt);
		phoneNum.set(split[1]);
		context.write(phoneNum, phoneFlow);
		
	}

}
