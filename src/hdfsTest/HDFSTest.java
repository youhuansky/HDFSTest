package hdfsTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * @Description：HDFS测试
 *                     <p>
 *                     创建日期：2018年5月2日
 *                     </p>
 * @version V1.0
 * @author Administrator
 * @see
 */
public class HDFSTest {

	/**
	 * @Description：上传文件
	 *                   <p>
	 *                   创建人：Administrator , 2018年5月2日 下午4:07:52
	 *                   </p>
	 *                   <p>
	 *                   修改人：Administrator , 2018年5月2日 下午4:07:52
	 *                   </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void getFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		fs.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\hdfs.txt"),
				new Path("/test/hdfs2.txt"));
		fs.close();
	}

	/**
	 * @Description：下载文件
	 *                   <p>
	 *                   创建人：Administrator , 2018年5月2日 下午4:08:56
	 *                   </p>
	 *                   <p>
	 *                   修改人：Administrator , 2018年5月2日 下午4:08:56
	 *                   </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void downfileFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		// 用两个参数（path,path）的报空指针异常
		fs.copyToLocalFile(false, new Path("/test/miaomiao.txt"),
				new Path("C:\\Users\\Administrator\\Desktop\\hadooptest\\miaomiao2.txt"), true);
		fs.close();
	}

	/**
	 * @Description：创建文件夹
	 *                    <p>
	 *                    创建人：Administrator , 2018年5月2日 下午4:19:52
	 *                    </p>
	 *                    <p>
	 *                    修改人：Administrator , 2018年5月2日 下午4:19:52
	 *                    </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void mkdirHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		fs.mkdirs(new Path("/test/test2"));
		fs.close();
	}

	/**
	 * @Description：删除文件
	 *                   <p>
	 *                   创建人：Administrator , 2018年5月2日 下午4:26:58
	 *                   </p>
	 *                   <p>
	 *                   修改人：Administrator , 2018年5月2日 下午4:26:58
	 *                   </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void delFileHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		fs.delete(new Path("/test2/test3"), false);
		fs.close();
	}

	/**
	 * @Description：重名名文件
	 *                    <p>
	 *                    创建人：Administrator , 2018年5月2日 下午4:39:31
	 *                    </p>
	 *                    <p>
	 *                    修改人：Administrator , 2018年5月2日 下午4:39:31
	 *                    </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void renameHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		fs.rename(new Path("/test/miaomiao.txt"), new Path("/test/miaomiao2.txt"));
		fs.close();
	}

	@Test
	public void listFileHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test/"), true);
		boolean hasNext = listFiles.hasNext();
		while (hasNext) {
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getBlockSize());
			System.out.println(next.getLen());
			System.out.println(next.getPath());

			System.out.println(next.getBlockSize());

			BlockLocation[] blockLocations = next.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				for (String string : hosts) {

					System.out.println(string);
				}
				System.out.println("----------------------------------");
			}

		}
		fs.close();
	}

	/**
	 * @Description：判断是文件还是文件夹
	 *                         <p>
	 *                         创建人：Administrator , 2018年5月2日 下午4:53:45
	 *                         </p>
	 *                         <p>
	 *                         修改人：Administrator , 2018年5月2日 下午4:53:45
	 *                         </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void judgeHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		FileStatus[] listStatus = fs.listStatus(new Path("/test/"));
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isDirectory()) {
				System.out.println("d--" + fileStatus.getPath().getName());
			} else {
				System.out.println("f--" + fileStatus.getPath().getName() + fileStatus.isFile());
			}
		}
		fs.close();
	}

	/**
	 * @Description：流的文件上传
	 *                     <p>
	 * 					创建人：Administrator , 2018年5月2日 下午5:37:59
	 *                     </p>
	 *                     <p>
	 * 					修改人：Administrator , 2018年5月2日 下午5:37:59
	 *                     </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void streamPushHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();

		FileSystem fs = null;
		FileInputStream fis = null;
		FSDataOutputStream fos = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
			fis = new FileInputStream(new File("C:\\Users\\Administrator\\Desktop\\hadooptest\\hdfs.txt"));
			fos = fs.create(new Path("/test/test2/hdfs3.txt"));
			IOUtils.copyBytes(fis, fos, configuration);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			fis.close();
			fos.close();
			fs.close();
		}
	}

	/**
	 * @Description：流的文件下载
	 *                     <p>
	 * 					创建人：Administrator , 2018年5月2日 下午5:40:24
	 *                     </p>
	 *                     <p>
	 * 					修改人：Administrator , 2018年5月2日 下午5:40:24
	 *                     </p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 *             void
	 */
	@Test
	public void streamPullHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();

		FileSystem fs = null;
		FSDataInputStream fis = null;
		FileOutputStream fos = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
			fis = fs.open(new Path("/test/test2/hdfs3.txt"));
			fos = new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\hadooptest\\hdfs4.txt"));
			IOUtils.copyBytes(fis, fos, configuration);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fis);
		}
	}
	
	
	/**
	 * @Description：定位读取文件
	 * <p>创建人：Administrator ,  2018年5月2日  下午5:55:06</p>
	 * <p>修改人：Administrator ,  2018年5月2日  下午5:55:06</p>
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 * void 
	 */
	@Test
	public void seekFileHDFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();

		FileSystem fs = null;
		FSDataInputStream fis = null;
		FileOutputStream fos = null;
		try {
			fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
			fis = fs.open(new Path("/test/test2/hdfs3.txt"));
			byte[] buf=new byte[1024];
//			for(int i=0;i<128*1024;i++) {
				fis.read(buf);
				
				IOUtils.copyBytes(fis, System.out, configuration);
//			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fis);
		}
	}
}
