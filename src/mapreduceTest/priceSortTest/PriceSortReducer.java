package mapreduceTest.priceSortTest;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PriceSortReducer extends Reducer<Product , Text, Text, Product> {

	int count=0;
	
	@Override
	protected void reduce(Product arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
//		if(count==0) {
//			arg2.write(arg1.iterator().next(), arg0);
//			count++;
//		}
		arg2.write(arg1.iterator().next(), arg0);
	}
}
