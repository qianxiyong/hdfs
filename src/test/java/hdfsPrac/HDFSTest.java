package hdfsPrac;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HDFSTest {
	
	@Test
	public void testMkdir() throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "atguigu");
		System.out.println(fileSystem.getClass().getName());
		boolean mkdirs = fileSystem.mkdirs(new Path("/eclipse"));
		if(mkdirs == true) {
			System.out.println("创建成功");
		}
		fileSystem.close();
	}
	
	@Test
	public void testMkdirTwo() throws IOException {
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hadoop101:9000");
		System.out.println(conf);
		FileSystem fileSystem = FileSystem.get(conf);
		boolean flag = fileSystem.mkdirs(new Path("/eclipse2"));
		if(flag == true) {
			System.out.println("创建成功");
		}
		fileSystem.close();
		
	}
	
	//演示在客户端指定备份为1 服务器的备份为3的情况 以客户端优先
	@Test
	public void test3() throws Exception {
		
		Configuration conf = new Configuration();
		System.out.println(conf);
		FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.copyFromLocalFile(false,true,new Path("E:/temp1/hadoop-2.7.2.tar.gz"), new Path("/eclipse2/"));
		fileSystem.close();
	}

}
