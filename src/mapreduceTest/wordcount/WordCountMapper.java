package mapreduceTest.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
KEYIN,  map的输入 键 LongWritable  行号
VALUEIN,map的输入 值 Text  一行的内容
KEYOUT, map的输出 键 Text  具体的单词
VALUEOUT,map的输出 值 LongWritable  单词的个数
*/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	
	Text text=new Text();
	LongWritable longWritable=new LongWritable(1);
	
	
	/**
	 *(非 Javadoc) 
	 * <p>Title: map</p> 
	 * <p>Description: </p> 文件每一行调用一次
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context) 
	 */ 
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//1.获取一行
		String line = value.toString();
		
		//2.用空格切割
		String[] split = line.split(" ");
		
		//3.发送出去
		for (String string : split) {
			text.set(string.getBytes());
			context.write(text,longWritable);
		}
		
	}
}
