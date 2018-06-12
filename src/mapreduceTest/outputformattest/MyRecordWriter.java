package mapreduceTest.outputformattest;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream createA;
	FSDataOutputStream createB;

	public MyRecordWriter(TaskAttemptContext context) throws IOException {

		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		createA = fileSystem.create(new Path("D:\\mapredtest\\outputtestresult\\atguigu.log"));
		createB = fileSystem.create(new Path("D:\\mapredtest\\outputtestresult\\other.log"));

	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		if (key.toString().contains("atguigu")) {
			createA.writeUTF(key.toString());
		}else {
			createB.writeUTF(key.toString());
			
		}
		
		
		
		
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		if(createA!=null) {
			createA.close();
		}
		
		if(createB!=null) {
			createB.close();
		}
	}

}
