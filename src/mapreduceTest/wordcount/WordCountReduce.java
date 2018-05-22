package mapreduceTest.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/*
KEYIN,  map的输入 键 Text  单词
VALUEIN,map的输入 值 LongWritable  
KEYOUT, map的输出 键 Text  单词
VALUEOUT,map的输出 值 LongWritable  单词的总个数
*/
public class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

	LongWritable sum=new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		//1.汇总
		long count=0;
		for (LongWritable longWritable : values) {
			count+=longWritable.get();
		}
		//2.输出
		sum.set(count);
		context.write(key, sum);
		
	}
}
