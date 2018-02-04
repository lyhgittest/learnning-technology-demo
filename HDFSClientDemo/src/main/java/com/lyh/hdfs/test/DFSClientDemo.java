package com.lyh.hdfs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

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

public class DFSClientDemo {

	private FileSystem fs;
	private Configuration conf;

	@Before
	public void before() throws Exception {
		conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://hadoop103:9000");
		// conf.set("dfs.replication", "2");
		// conf.set("HADOOP_USER_NAME", "lyh");
		// fs = FileSystem.get(conf);//这种方式获取文件系统如何设置用户呢？
		fs = FileSystem.get(new URI("hdfs://hadoop103:9000"), conf, "lyh");// 这种方式可以设置用户
	}

	// 创建文件夹
	@Test
	public void test1() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/user/zhangsan/lisi"));
		/*
		 * System.setProperty("HADOOP_USER_NAME", "lyh"); Map<String, String>
		 * map = System.getenv(); Set<Entry<String, String>> entrySet =
		 * map.entrySet(); for (Entry<String, String> entry : entrySet) {
		 * System.out.println(entry.getKey()+"--"+entry.getValue()); }
		 * System.out.println("HADOOP_USER_NAME:"+System.getenv(
		 * "HADOOP_USER_NAME"));
		 * System.out.println("HADOOP_USER_NAME:"+System.getProperty(
		 * "HADOOP_USER_NAME"));
		 */
	}

	// 上传文件
	@Test
	public void test2() throws IllegalArgumentException, IOException {

		fs.copyFromLocalFile(new Path("d:/dp/abc.txt"), new Path("/user/input"));
	}

	// 显示文件和文件夹的详细信息
	@Test
	public void test3() throws IllegalArgumentException, IOException {
		// 获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			// 输出文件名称
			System.out.println(fileStatus.getPath().getName());
			//
			System.out.println(fileStatus.getPath());
			// 长度
			System.out.println(fileStatus.getLen());// 33617
			// 权限
			System.out.println(fileStatus.getPermission());// rwxrwx---
			// 所属组
			// System.out.println(fileStatus.getGroup());//supergroup
			// 获取存储块的信息
			BlockLocation[] locations = fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : locations) {
				// 获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
		}
		System.out.println(fs);// job_1517618752984_0001.summary
	}

	// 文件与文件夹的判断
	@Test
	public void test4() throws IllegalArgumentException, IOException {
		// 判断是文件还是文件夹
		// 获取文件的状态数组
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				// 是文件
				System.out.println("f:" + fileStatus.getPath().getName());
			} else {
				// 是文件夹
				System.out.println("d:" + fileStatus.getPath().getName());
			}
		}
	}

	// HDFS的IO流操作,文件的上传
	@Test
	public void test5() {

		// 创建输入流
		FileInputStream fis;
		// 获取输出流，跟HDFS交互，需要FSData
		FSDataOutputStream fos;
		try {
			// 从本地系统获取要上传的文件
			fis = new FileInputStream(new File("d:/dp/abc.txt"));
			// 将文件上传到指定的路径上
			fos = fs.create(new Path("/user/zhangsan/test.txt"));
			// 流对拷
			IOUtils.copyBytes(fis, fos, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// HDFS的文件下载
	@Test
	public void test6() {

		// 创建输入流
		FSDataInputStream fis = null;
		// 获取输出流
		FileOutputStream fos = null;
		try {
			fis = fs.open(new Path("/user/zhangsan/test.txt"));
			fos = new FileOutputStream(new File("d:/dp/mng.txt"));
			// 流对拷
			IOUtils.copyBytes(fis, fos, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fis);
		}
	}

	// 定位文件读取
	@Test
	public void test7() throws IllegalArgumentException, IOException {
		// 定位下载第一块
		// 创建输入流
		FSDataInputStream fis = null;
		// 获取输出流
		FileOutputStream fos = null;
		try {
			fis = fs.open(new Path("/user/zhangsan/hadoop-2.7.2.tar.gz"));
			fos = new FileOutputStream(new File("d:/dp/hadoop-2.7.2.tar.gz.part2"));
			// 流的拷贝
			byte[] buf = new byte[1024];
			int len = -1;
			for (int i = 0; i < 1024 * 128; i++) {
				/*
				 * while ((len = fis.read(buf)) != -1) { fos.write(buf, 0, len);
				 * }
				 */
				fis.read(buf);
				fos.write(buf);
			}
			// 流对拷
			// IOUtils.copyBytes(fis, fos, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fis);
		}
	}

	// 定位文件读取,下载第二块文件
	@Test
	public void test8() throws IllegalArgumentException, IOException {
		// 定位下载第二块
		// 创建输入流
		FSDataInputStream fis = null;
		// 获取输出流
		FileOutputStream fos = null;
		try {
			fis = fs.open(new Path("/user/zhangsan/hadoop-2.7.2.tar.gz"));

			// 定位资源
			fis.seek(1024 * 1024 * 128);
			fos = new FileOutputStream(new File("d:/dp/hadoop-2.7.2.tar.gz.part3"));
			// 流对拷
			IOUtils.copyBytes(fis, fos, conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fos);
			IOUtils.closeStream(fis);
		}
	}

	// HDFS文件名修改
	@Test
	public void test9() throws IllegalArgumentException, IOException {

		fs.rename(new Path("/user/zhangsan/test.txt"), new Path("/user/zhangsan/test2.txt"));
	}

	@After
	public void destory() throws IOException {
		if (fs != null) {
			System.out.println("over!!!");
			fs.close();
		}
	}
}
