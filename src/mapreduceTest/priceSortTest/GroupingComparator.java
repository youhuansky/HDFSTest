package mapreduceTest.priceSortTest;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{

	protected GroupingComparator() {
		super(Product.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Product product1=(Product)a;
		Product product2=(Product)b;
		return -product1.getPrice().compareTo(product2.getPrice());
	}
}
