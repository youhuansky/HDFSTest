package hdfsTest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HDFSTest {
	
	@Test
	public void getFile() throws IOException, InterruptedException, URISyntaxException{
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		fs.copyFromLocalFile(new Path("C:\\Users\\HP\\Desktop\\编程工具\\hdfstest\\hdfstest.txt"), new Path("/HDFSTEST/hdfs1.txt"));
		
	}
}
