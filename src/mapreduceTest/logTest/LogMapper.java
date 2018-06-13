package mapreduceTest.logTest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String string = value.toString();

		boolean flag = cleanDate(string, context);
		
		
		
		if(flag) {
			
			 context.write(new Text(string), NullWritable.get());
		}

	}

	/**
	 * @Description：清洗数据，对11长度一下的字段删除掉
	 *                                 <p>
	 * 								创建人：Administrator , 2018年6月13日 下午2:22:39
	 *                                 </p>
	 *                                 <p>
	 * 								修改人：Administrator , 2018年6月13日 下午2:22:39
	 *                                 </p>
	 *
	 * @param string
	 * @param context
	 * @return boolean
	 */
	private boolean cleanDate(String string, Context context) {

		boolean flag = false;

		Counter counter = context.getCounter("logGroup", "logGroup_false");

		String[] split = string.split(" ");
		if (split.length <= 11) {
			flag = false;

			counter.increment(1);

		} else {

			flag = true;
		}

		return flag;
	}

}
