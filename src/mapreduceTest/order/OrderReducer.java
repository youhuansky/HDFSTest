package mapreduceTest.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

	
	@Override
	protected void reduce(Text key, Iterable<TableBean> value,
			Context context) throws IOException, InterruptedException {
		
		List<TableBean> tableBeanList = new ArrayList<TableBean>();
		
		TableBean tableBean =new TableBean();
		
		for (TableBean eachTableBean : value) {
			String flag = eachTableBean.getFlag();
			if("0".equals(flag)) {
				tableBean=eachTableBean;
			}else {
				tableBeanList.add(eachTableBean);
			}
		}
		
		for (TableBean eachTableBean : tableBeanList) {
			eachTableBean.setPname(tableBean.getPname());
			context.write(eachTableBean, NullWritable.get());
		}
		
		
	}
}
