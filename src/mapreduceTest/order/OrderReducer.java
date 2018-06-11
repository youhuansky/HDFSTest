package mapreduceTest.order;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

	
	@Override
	protected void reduce(Text key, Iterable<TableBean> value,
			Context context) throws IOException, InterruptedException {
		
		List<TableBean> tableBeanList = new ArrayList<TableBean>();
		
		TableBean tableBean =new TableBean();
		
		TableBean tmp =new TableBean();
		for (TableBean eachTableBean : value) {
			TableBean tableBean2 =new TableBean();
			String flag = eachTableBean.getFlag();
			if("0".equals(flag)) {
				try {
					BeanUtils.copyProperties(tableBean, eachTableBean);
				} catch (Exception e) {
					e.printStackTrace();
				} 
//				tableBean=eachTableBean;
			}else {
				try {
					BeanUtils.copyProperties(tableBean2, eachTableBean);
				} catch (Exception e) {
					e.printStackTrace();
				} 
//				tableBean2=eachTableBean;
				tableBeanList.add(tableBean2);
			}
		}
		
		for (TableBean eachTableBean2 : tableBeanList) {
			eachTableBean2.setPname(tableBean.getPname());
			context.write(eachTableBean2, NullWritable.get());
		}
		
		
	}
}
