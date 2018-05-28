package mapreduceTest.flowSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PhoneFlow implements WritableComparable<PhoneFlow>{
	
	
	private  int upFlow;
	private  int downFlow;
	private  int sumFlow;
	
	
	public int getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
	}
	public int getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(int downFlow) {
		this.downFlow = downFlow;
	}
	public int getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(int sumFlow) {
		this.sumFlow = sumFlow;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow=in.readInt();
		downFlow=in.readInt();
		sumFlow=in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upFlow);
		out.writeInt(downFlow);
		out.writeInt(sumFlow);
	}
	@Override
	public String toString() {
		return  upFlow + " " + downFlow + " " + sumFlow;
	}
	
	
	
	
	/**
	 *(非 Javadoc) 
	 * <p>Title: compareTo</p> 
	 * Description:和本对象比较
	 * @param o
	 * @return 
	 * @see java.lang.Comparable#compareTo(java.lang.Object) 
	 */ 
	@Override
	public int compareTo(PhoneFlow o) {
		
		return sumFlow>o.getSumFlow()?-1:1;
	}
	
	
}
