package mapreduceTest.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;

public class Decompress {

	
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		decompress();
		
		
	}

	private static void decompress() throws FileNotFoundException, IOException {

		String string="D:\\mapredtest\\indexonetest\\a.txt.gz";
		CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(new Configuration());
		CompressionCodec codec = compressionCodecFactory.getCodec(new Path(string));
		if(codec==null) {
			System.out.println("meiyou");
			return ;
			
		}
		CompressionInputStream createInputStream = codec.createInputStream(new FileInputStream(new File(string)));
		
		FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\\\mapredtest\\\\indexonetest\\\\d.txt"));
		
		
		IOUtils.copyBytes(createInputStream, fileOutputStream, 1024*1024,false);
		
		
		createInputStream.close();
		fileOutputStream.close();
	}
}
