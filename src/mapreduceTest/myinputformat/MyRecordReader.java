package mapreduceTest.myinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

	BytesWritable value = new BytesWritable();
	FileSplit split;
	Configuration configuration;
	private boolean isProcessed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 初始化
		this.split = (FileSplit) split;
		configuration = context.getConfiguration();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// 业务逻辑处理
		boolean isResult;
		if (!isProcessed) {

			byte[] buf = new byte[(int) split.getLength()];
			FileSystem fs = FileSystem.get(configuration);
			Path path = split.getPath();
			FSDataInputStream open=null ;
			try {
				 open = fs.open(path);
				// 流的拷贝
				IOUtils.readFully(open, buf, 0, buf.length);
				
				
				//设置输出值
				value.set(buf, 0, buf.length);
			} finally {
				IOUtils.closeStream(open);
			}
		
			isProcessed = true;
			isResult = true;
		} else {

			isResult = false;
		}

		return isResult;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return isProcessed?1:0;
	}

	@Override
	public void close() throws IOException {

	}

}
