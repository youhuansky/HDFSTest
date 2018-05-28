package mapreduceTest.phoneflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class PhoneFlow implements Writable{
	
	
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
	
	
}
