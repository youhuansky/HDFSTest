package mapreduceTest.priceSortTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProdtSortPartitioner extends Partitioner<Product , Text> {

	@Override
	public int getPartition(Product key, Text value, int numPartitions) {
		String string = value.toString();
		int partNum=0;
		if("0000002".equals(string)) {
			partNum=2;
		}else if ("0000001".equals(string)) {
			partNum=1;
		}else if ("0000003".equals(string)) {
			partNum=0;
		}
		
		return partNum;
	}

	
}
