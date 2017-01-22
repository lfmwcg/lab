package li.mt;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class DoHDFS {

	public static void readFromHdfs() throws FileNotFoundException, IOException {
		String dst = "hdfs://hadoop0:9000/d/test.txt";
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));

		OutputStream out = new FileOutputStream("/usr/local/lab/qq-hdfs.txt");
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {
			out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		out.close();
		hdfsInStream.close();
		fs.close();
	}

	/** 遍历HDFS上的文件和目录 */
	public static void getDirectoryFromHdfs() throws FileNotFoundException, IOException {
		String dst = "hdfs://hadoop0:9000/d/";
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus fileList[] = fs.listStatus(new Path(dst));
		int size = fileList.length;
		for (int i = 0; i < size; i++) {
			System.out.println("name:" + fileList[i].getPath().getName() + "/t/tsize:" + fileList[i].getLen());
		}
		fs.close();
	}

	/** 上传文件到HDFS上去 */
	public static void uploadToHdfs() throws FileNotFoundException, IOException {
		String localSrc = "/usr/local/lab/d/test.zip";
		String dst = "hdfs://hadoop0:9000/d/tmp.zip";

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}

	/** 以append方式将内容添加到HDFS上文件的末尾;注意：文件更新，需要在hdfs-site.xml中添<property><name>dfs.append.support</name><value>true</value></property> */
	public static void appendToHdfs() throws FileNotFoundException, IOException {
		String dst = "hdfs://hadoop0:9000/d/test.txt";
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataOutputStream out = fs.append(new Path(dst));

		int readLen = "zhangzk add by hdfs Java api".getBytes().length;

		while (-1 != readLen) {
			out.write("zhangzk add by hdfs java api".getBytes(), 0, readLen);
		}
		out.close();
		fs.close();
	}

	/** 从HDFS上删除文件 */
	public static void deleteFromHdfs() throws FileNotFoundException, IOException {
		String dst = "hdfs://hadoop0:9000/d/test.txt";
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		fs.deleteOnExit(new Path(dst));
		fs.close();
	}

	public static void main(String[] args) {
		// Create a Parser
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "Print this usage information");
		options.addOption("c", "call", true, "call functions");

		// print usage
		HelpFormatter formatter = new HelpFormatter();

		try {
			// Parse the program arguments
			CommandLine commandLine = parser.parse(options, args);

			// Set the appropriate variables based on supplied options
			String callf = "";

			if (commandLine.hasOption('c')) {
				callf = commandLine.getOptionValue('c');
				System.out.println("call functions:" + callf);
				int called = 0;
				try {
					Class<?> c = Class.forName("li.mtt.DoHDFS");
					Method[] m = c.getMethods();
					for (int i = 0; i < m.length; i++) {
						if (m[i].getName().equals(callf)) {
							m[i].invoke(null);
							called = 1;
						}
					}
					if (called == 0) {
						for (int i = 0; i < m.length; i++) {
							System.out.println(m[i].getName());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else if (commandLine.hasOption('h')) {
				System.out.println("Help Message");
				formatter.printHelp("java -jar XXX.jar", options);
				System.out.println();
				System.exit(0);
			} else {
				formatter.printHelp("java -jar XXX.jar", options);
				System.exit(0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
