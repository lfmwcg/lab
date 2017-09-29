package trackProc;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.TableName;

public class HbaseInput {
	private static Configuration conf = null;
	private static Connection conn = null;

	public static void init() throws Exception {
		System.out.println("enter init.");
		conf = HBaseConfiguration.create();

		conf.set("hbase.rootdir", "hdfs://runs25159:8020/apps/hbase/data");
		conf.set("hbase.zookeeper.quorum", "runs25160,runs25161,runs25159");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		conn = ConnectionFactory.createConnection(conf);
		System.out.println("create connection " + conn.toString());

	}

	public static void showTable() {
		System.out.println("enter showTable.");
		Admin admin = null;
		try {
			System.out.println("12312");
			admin = conn.getAdmin();
			System.out.println("33333");
			TableName[] tbln = admin.listTableNames();
			System.out.println("hbase tables:" + tbln.length);
			for (int t = 0; t < tbln.length; t++) {
				String tablen = tbln[t].getNameAsString();
				System.out.println("hbase tables:" + tablen);
				// HTableDescriptor tabled = admin.getTableDescriptor(tbln[t]);
				// System.out.println(tabled.getFamilies().toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void createTable() {
		System.out.println("Entering testCreateTable.");
		TableName tableName = TableName.valueOf("obj_track_01");

		// Specify the table descriptor.
		HTableDescriptor htd = new HTableDescriptor(tableName);
		
		// Set the column family name to info.
		HColumnDescriptor hcd = new HColumnDescriptor("t");
		
		// Set data encoding methods��HBase provides DIFF,FAST_DIFF,PREFIX
		// and PREFIX_TREE
		hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

		// Set compression methods, HBase provides two default compression
		// methods:GZ and SNAPPY
		// GZ has the highest compression rate,but low compression and
		// decompression effeciency,fit for cold data
		// SNAPPY has low compression rate, but high compression and
		// decompression effeciency,fit for hot data.
		// it is advised to use SANPPY
		hcd.setCompressionType(Compression.Algorithm.SNAPPY);
		// hcd.setEncryptionType(Bytes.toBytes("AES"));
		// hcd.setEncryptionKey(Bytes.toBytes("AES"));

		htd.addFamily(hcd);

		Admin admin = null;
		try {
			// Instantiate an Admin object.
			admin = conn.getAdmin();
			if (null == admin){
				System.out.println("admin is null ");
			}
			admin.createTable(htd);
/*			if (!admin.tableExists(tableName)) {
				System.out.println("Creating table...");
				
				System.out.println(admin.getClusterStatus());
				System.out.println(admin.listNamespaceDescriptors());
				System.out.println("Table created successfully.");
			} else {
				System.out.println("table already exists");
			}*/
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Create table failed.");
		} finally {
			if (admin != null) {
				try {
					// Close the Admin object.
					admin.close();
				} catch (IOException e) {
					System.out.println("Failed to close admin ");
					e.printStackTrace();
				}
			}
		}
		System.out.println("Exiting testCreateTable.");
	}

	// 解决报错找不到hadoop.home.dir的问题,入Hbase和HDFS需要调用
	public static void createWinutils() {
		File workaround = new File(".");
		System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
		new File("./bin").mkdirs();
		try {
			new File("./bin/winutils.exe").createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		createWinutils();
		try {
			init();
		} catch (Exception e) {
			e.printStackTrace();
		}
		createTable();
		showTable();


	}
}
