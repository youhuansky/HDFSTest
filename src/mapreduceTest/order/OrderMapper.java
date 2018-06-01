package mapreduceTest.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OrderMapper extends Mapper<LongWritable, Text, Text, TableBean> {

	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit)context.getInputSplit();
		String name = inputSplit.getPath().getName();
		String string = value.toString();
		String[] split = string.split("\\ ");
		TableBean tableBean = new TableBean();
		String pid="";
		if(name.startsWith("order")) {
			tableBean.setId(split[0]);
			tableBean.setPid(split[1]);
			tableBean.setAmount(Double.valueOf(split[2]));
			tableBean.setFlag("1");
			tableBean.setPname("");
			pid=split[1];
		}else {
			tableBean.setPid(split[0]);
			tableBean.setPname(split[1]);
			tableBean.setFlag("0");
			tableBean.setId("");
			tableBean.setAmount(0.00);
			pid=split[0];
		}
		context.write(new Text(pid), tableBean);
		
	}
}
