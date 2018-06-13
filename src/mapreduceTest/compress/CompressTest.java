package mapreduceTest.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressTest {

	
	
	public static void main(String[] args) throws FileNotFoundException, Exception {
		compress();
		
		
		
	}

	private static void compress() throws FileNotFoundException, Exception {
		String fileName="D:\\mapredtest\\indexonetest\\a.txt";
		//1.获取输入流
		FileInputStream fileInputStream = new FileInputStream(new File(fileName));
		//2.获取输出流
//		Class forName = Class.forName("org.apache.hadoop.io.compress.BZip2Codec");
		Class forName = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
		CompressionCodec newInstance = (CompressionCodec)ReflectionUtils.newInstance(forName, new Configuration());
//		CompressionCodec newInstance = (CompressionCodec)forName.newInstance();
		FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName+newInstance.getDefaultExtension()));
		//3.把输出流封装成压缩流
		CompressionOutputStream createOutputStream = newInstance.createOutputStream(fileOutputStream);
		//4.流的对拷贝
		IOUtils.copyBytes(fileInputStream, createOutputStream,1024*1024,false);
		
		fileInputStream.close();
		IOUtils.closeStream(createOutputStream);
		IOUtils.closeStream(fileOutputStream);
		
	}
}
