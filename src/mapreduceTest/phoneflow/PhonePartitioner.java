package mapreduceTest.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner extends Partitioner<Text, PhoneFlow>{

	@Override
	public int getPartition(Text key, PhoneFlow value, int numPartitions) {
		String substring = key.toString().substring(0, 3);
		System.out.println("本流程的分块的int是"+numPartitions);
		int partNum=4;
		if("136".equals(substring)) {
			partNum=0;
		}else if ("137".equals(substring)) {
			partNum=1;
		}else if ("138".equals(substring)) {
			partNum=2;
		}else if ("139".equals(substring)) {
			partNum=3;
		}
		
		
		return partNum;
	}

}
