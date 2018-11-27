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

}
