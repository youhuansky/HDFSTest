package mapreduceTest.priceSortTest;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PriceSortMapper extends Mapper<LongWritable, Text,Product , Text> {

	Product product=new Product();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String string = value.toString();
		String[] split = string.split("\\ ");
		
		product.setProdtName(split[1]);
		product.setPrice(new BigDecimal(split[2]));
		
		context.write(product, new Text(split[0]));
	}
}
