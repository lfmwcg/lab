package li.mt;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

public class DoParquet {
	public static final MessageType FILE_SCHEMA = Types.buildMessage()
			.required(PrimitiveTypeName.INT32).named("id")
			.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
			.named("test");

	public static void createFile() throws Exception {
		String dst = "hdfs://hadoop0:9000/d/test.pq";
		Path path = new Path(dst);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		SimpleGroupFactory f = new SimpleGroupFactory(FILE_SCHEMA);
		ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).withType(FILE_SCHEMA).build();
		for (int i = 0; i < 100; i++) {
			Group group = f.newGroup();
			group.add("id", i);
			group.add("name", UUID.randomUUID().toString());
			writer.write(group);
		}
		writer.close();
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
					Class<?> c = Class.forName("li.mtt.DoParquet");
					Method[] m = c.getMethods();
					for (int i = 0; i < m.length; i++) {
						if (m[i].getName().equals(callf)) {
							m[i].invoke(null);
							called = 1;
						}
					}
					if (called == 0){
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