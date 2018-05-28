package mapreduceTest.flowSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhoneSortPartioner extends Partitioner<PhoneFlow,Text> {

	@Override
	public int getPartition(PhoneFlow value,Text key,  int numPartitions) {
		int partnum=2;
		String string = key.toString();
		String substring = string.substring(0,3);
		if("137".equals(substring)) {
			
			partnum=0;
		}else if ("136".equals(substring)) {
			partnum=1;
			
		}
		return partnum;
	}

}
