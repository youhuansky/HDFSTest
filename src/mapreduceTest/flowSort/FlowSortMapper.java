package mapreduceTest.flowSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSortMapper extends Mapper<LongWritable, Text, PhoneFlow, Text> {

	PhoneFlow phoneFlow=new PhoneFlow();
	int upFlow;
	int downFlow;
	int sumFlow;
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String string = value.toString();
		String[] split = string.split("\\ ");
		upFlow=Integer.valueOf(split[1]);
		downFlow=Integer.valueOf(split[2]);
		sumFlow=Integer.valueOf(split[3]);
		phoneFlow.setUpFlow(upFlow);
		phoneFlow.setDownFlow(downFlow);
		phoneFlow.setSumFlow(sumFlow);
		
		context.write(phoneFlow, new Text(split[0]));
		
	}
}
