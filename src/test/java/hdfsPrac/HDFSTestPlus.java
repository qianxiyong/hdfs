package hdfsPrac;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSTestPlus {

	private Configuration conf;
	private FileSystem fs;

	@Before
	public void init() throws IOException {

		conf = new Configuration();
		fs = FileSystem.get(conf);
	}

	@After
	public void close() throws Exception {
		fs.close();
	}

	// 测试下载
	@Test
	public void testDownLoad() throws Exception {
		fs.copyToLocalFile(false, new Path("/input/test.txt"), new Path("E:/"), true);
	}

	// 测试删除文件
	@Test
	public void testRemoveFile() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/eclipse/hadoop-2.7.2.tar.gz"), true);
	}

	// 测试删除目录
	@Test
	public void testRemoveDir() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/eclipse2"), true);
	}
	
	// 判断path是目录还是文件
	// 使用getFileStatus，先获取path对应的FileStatus，在通过此对象，调用方法判断！
	// 如果你需要遍历当前路径的所有子目录和文件，使用listStatus
	// 如果不需要遍历子目录，使用getFileStatus
	@Test
	public void testIsFileOrDic() throws Exception, IOException {
		FileStatus fileStatus = fs.getFileStatus(new Path("/test"));
		//boolean directory = fileStatus.isDirectory();
		if(fileStatus.isDirectory()) {
			System.out.println("/test是一个目录");
			FileStatus[] listStatus = fs.listStatus(new Path("/test"));
			for (FileStatus status : listStatus) {
				if(status.isDirectory()) {
					System.out.println(status.getPath().getName()+"是一个文件夹");
				}
				if(status.isFile()) {
					System.out.println(status.getPath().getName()+"是一个文件");
				}
			}
		}
	    if(fileStatus.isFile()) {
	    	System.out.println("/test是哪一个文件");
	    }
	}
	
	// 真多一个超过128MB的文件，如何实现指定下载某一块
	//下载第一块
	@SuppressWarnings("resource")
	@Test
	public void testFirstBlock() throws Exception, IOException {
		FSDataInputStream open = fs.open(new Path("/test/hadoop-2.7.2.tar.gz"));
		FileOutputStream fileOutputStream = new FileOutputStream("E:/firstBlock");
		
		byte[] b = new byte[1024];
		
		for(int i = 0;i < 1024 * 128;i++) {
			open.read(b);
			fileOutputStream.write(b);
		}
		
		IOUtils.closeStream(open);
		IOUtils.closeStream(fileOutputStream);
	}
	
	@SuppressWarnings("resource")
	@Test
	//下载第二块
	//windows拼接 type >>
	//linux拼接 cat >>
	public void testSecondBlock() throws IllegalArgumentException, IOException {
		FSDataInputStream fsDataInputStream = fs.open(new Path("/test/hadoop-2.7.2.tar.gz"));
		FileOutputStream fileOutputStream = new FileOutputStream("E:/secondBlock");
		fsDataInputStream.seek(1024*1024*128);
		IOUtils.copyBytes(fsDataInputStream, fileOutputStream, conf, true);
	}
	
	// 自己实现文件的上传和下载
	
		/*
		 *   in = srcFS.open(src);
		 *   		使用FileSystem.open()可以获取文件系统上的一个输入流
	        out = dstFS.create(dst, overwrite);
	        		使用FileSystem.create()可以获取文件系统上的一个输出流
	        IOUtils.copyBytes(in, out, conf, true);
	        		使用hadoop提供的IOUtils可以完成流的拷贝
	        			最后一个参数true代表，拷贝完成后关闭流
		 */
	@Test
	public void testMyUpLoad() throws Exception {
		InputStream is = new FileInputStream("E:/test.txt");
		FSDataOutputStream create = fs.create(new Path("/test/test.txt"), true);
		IOUtils.copyBytes(is, create, conf, true);
	}
	
	@Test
	public void testMyDownLoad() throws IllegalArgumentException, IOException {
		FSDataInputStream open = fs.open(new Path("/test/test.txt"));
		FileOutputStream outputStream = new FileOutputStream("E:/a.txt");
		IOUtils.copyBytes(open, outputStream, conf, true);
	}
	// 查看文件状态，查看文件的元数据
	@Test
	public void testListStatus() throws Exception, IllegalArgumentException, IOException {
		
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println("--------------------------");
			System.out.println("Permission:"+fileStatus.getPermission());
			System.out.println("Ownner:"+fileStatus.getOwner());
			System.out.println("Group:"+fileStatus.getGroup());
			System.out.println("Size:"+fileStatus.getLen());
			System.out.println("Last Modified:"+fileStatus.getModificationTime());
			System.out.println("Replication:"+fileStatus.getReplication());
			System.out.println("Block Size:"+fileStatus.getBlockSize());
			System.out.println("Name:"+fileStatus.getPath().getName());
			
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				
				List<String> asList = Arrays.asList(blockLocation.getNames());
				System.out.println("块存储的DN:" + asList);
				System.out.println("块的大小:"+blockLocation.getLength());
				System.out.println("块存储数据的起始字节:"+blockLocation.getOffset());
				System.out.println("块存储的主机名:"+Arrays.asList(blockLocation.getHosts()));
				System.out.println("------------------下一块------------------");
			}
			
		}
	}
}
