package mapreduceTest.priceSortTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.WritableComparable;

public class Product implements WritableComparable<Product> {

	private String prodtName;
	private BigDecimal price;
	
	
	
	public String getProdtName() {
		return prodtName;
	}

	public void setProdtName(String prodtName) {
		this.prodtName = prodtName;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(prodtName.getBytes());
		out.write(("-"+price.toString()).getBytes());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String tmp=in.readLine();
		String[] split = tmp.split("-");
		this.prodtName=split[0];
		price=new BigDecimal(split[1]);
		
	}

	@Override
	public int compareTo(Product o) {
		return o.getPrice().compareTo(this.price);
	}

	@Override
	public String toString() {
		return  prodtName + "\t" + price ;
	}

	
}
