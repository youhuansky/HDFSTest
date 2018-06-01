package mapreduceTest.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TableBean implements Writable {
	
	private String id;
	private String pid;
	private String pname;
	private Double amount;
	private String flag;
	
	
	
	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(pid);
		out.writeUTF(pname);
		out.writeDouble(amount);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id=in.readUTF();
		pid=in.readUTF();
		pname=in.readUTF();
		amount=in.readDouble();
		flag = in.readUTF();
	}

	@Override
	public String toString() {
		return id + "\t" + pname + "\t" + amount;
	}

	
	
	
}
