package mapreduceTest.order.distribute;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mapreduceTest.order.TableBean;

public class OrderDistributeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Map<String, String> hashMap = new HashMap<String,String>();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		InputStream fileInputStream = new FileInputStream(new File("pd.txt"));
		BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
		
		String readLine;
		while(StringUtils.isNotEmpty(readLine = br.readLine()) ) {
			String[] split = readLine.split("\\ ");
			
			hashMap.put(split[0], split[1]);
			
		}
		
	}
	
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String string = value.toString();
		String[] split = string.split("\\ ");
		
		TableBean tableBean = new TableBean();
		tableBean.setId(split[0]);
		
		tableBean.setPname(hashMap.get(split[1]));
		
		tableBean.setAmount(Double.valueOf(split[2]));
		
		
		context.write(new Text(tableBean.toString()), NullWritable.get());
		
	}
}
