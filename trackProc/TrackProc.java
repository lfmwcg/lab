package trackProc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

public class TrackProc {
	private static HashSet<String> keyset = new HashSet<String>();
	private static int keysetN = 0;
	private static int totalkeyN = 0;

	public static void getKV(String str) {
		// Map<String,String> map = new HashMap<String,String>();
		String key = "";
		String time = "";
		String s1[] = str.split("\t");

		key = s1[1] + "-" + s1[2] + "-" + s1[3] + "-" + s1[4];
		time = s1[0];
		keyset.add(key);
		// map.put(key, time);

	}

	public static void writeKeyset(HashSet<String> set) {
		String dir = Configs.readValue("config/track.cfg", "data.dir");
		FileWriter fw = null;
		try {
			fw = new FileWriter(dir + "keyset" + keysetN + ".key");
			keysetN++;
			for (String key : set) {
				fw.write(key + "\n");
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void writeKeyset(HashSet<String> set, int filenum) {
		String dir = Configs.readValue("config/track.cfg", "key_u.dir");
		FileWriter fw = null;
		try {
			fw = new FileWriter(dir + "keyset" + filenum + ".key");
			for (String key : set) {
				fw.write(key + "\n");
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void readkey() {

		String keyfile = Configs.readValue("config/track.cfg", "key.file");
		int sed = 9;
		for (int ih = -sed; ih < sed; ih++) {
			HashSet<String> localkey = new HashSet<String>();
			int ln = 0;
			int totalln = 0;
			try {
				File file = new File(keyfile);
				if (file.isFile() && file.exists()) { // 判断文件是否存在
					InputStreamReader read = new InputStreamReader(new FileInputStream(file));// 考虑到编码格式
					BufferedReader bufferedReader = new BufferedReader(read);
					String lineTxt = null;
					while ((lineTxt = bufferedReader.readLine()) != null) {
						ln++;
						totalln++;
						int code = lineTxt.hashCode();
						if (code % sed == ih) {
							// System.out.println("code:" + code + ",code%5:" +
							// code
							// % 5);

							localkey.add(lineTxt.replaceAll("\t", "").trim());
						}
						if (ln > 500000) {
							ln = 0;
							System.out.println("proc file lines:" + totalln + "set size:" + localkey.size());
						}
					}
					bufferedReader.close();
					read.close();
					System.out.println("set size:" + localkey.size());
					writeKeyset(localkey, ih);

				} else {
					System.out.println("找不到指定的文件");
				}
			} catch (Exception e) {
				System.out.println("读取文件内容出错");
				e.printStackTrace();
			}
		}

	}

	public static void procDataFile(String filePath) {
		int lineInfile = 0;

		try {
			System.out.println("prock file:" + filePath);
			File file = new File(filePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file));// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					lineInfile++;
					// System.out.println(lineTxt);
					getKV(lineTxt);

				}
				bufferedReader.close();
				read.close();
				System.out.println("proc file lines:" + lineInfile);
				totalkeyN += keyset.size();
				System.out.println("key num:" + keyset.size());
				System.out.println("totalkeyN:" + totalkeyN);
				if (keyset.size() > 5000000) {
					writeKeyset(keyset);
					keyset.clear();
				}
			} else {
				System.out.println("找不到指定的文件");
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}

	}

	public static void procDir() {
		String dir = Configs.readValue("config/track.cfg", "data.dir");
		File file = new File(dir);
		String[] fileName = file.list();
		for (String strf : fileName) {
			// System.out.println(strf);
			if (strf.contains(".bcp")) {
				procDataFile(dir + strf);

			}
		}

	}

	public static void findAndWrite(String strline, HashSet<String> hset, Map<String, String> outmap) {
		String strk = "";
		String time = "";
		String s1[] = strline.split("\t");

		strk = s1[1] + "-" + s1[2] + "-" + s1[3] + "-" + s1[4];
		time = s1[0];

		if (hset.contains(strk)) {
			String tstr = outmap.get(strk);
			if (null == tstr) {
				outmap.put(strk, time);
			} else {
				tstr = tstr + "," + time;
				outmap.put(strk, tstr);
			}
		}

	}
	
	public static void splitLineAndCount(String strline, HashSet<String> hset) {
		String s1[] = strline.split("\t");

		//strk = s1[1] + "-" + s1[2] + "-" + s1[3] + "-" + s1[4];
		//time = s1[0];

		hset.add(s1[1]);
	}

	public static void procOnekey(HashSet<String> hset, FileWriter fw) {

		String dir = Configs.readValue("config/track.cfg", "source.dir");
		Map<String, String> outMap = new HashMap<String, String>();
		File file = new File(dir);
		String[] fileName = file.list();
		for (String strf : fileName) {

			try {
				System.out.println("prock file:" + strf);
				File sfile = new File(dir + strf);
				if (sfile.isFile() && sfile.exists()) { // 判断文件是否存在
					InputStreamReader read = new InputStreamReader(new FileInputStream(sfile));// 考虑到编码格式
					BufferedReader bufferedReader = new BufferedReader(read);
					String lineTxt = null;
					while ((lineTxt = bufferedReader.readLine()) != null) {
						// System.out.println(lineTxt);
						lineTxt.split("\t");
						
					}
					bufferedReader.close();
					read.close();

				}
				System.out.println("outMap size:" + outMap.size());

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		try {
			for (Entry<String, String> entry : outMap.entrySet()) {
				// System.out.println("key" + entry.getKey() + ",time:" +
				// entry.getValue());
				fw.write(entry.getKey() + "\t" + entry.getValue()+"\n");

			}
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void countTagValueNum() {

		String dir = Configs.readValue("config/track.cfg", "source.dir");
		HashSet<String> countSet = new HashSet<String>();
		File file = new File(dir);
		String[] fileName = file.list();
		for (String strf : fileName) {

			try {
				System.out.println("prock file:" + strf);
				File sfile = new File(dir + strf);
				if (sfile.isFile() && sfile.exists()) { // 判断文件是否存在
					InputStreamReader read = new InputStreamReader(new FileInputStream(sfile));// 考虑到编码格式
					BufferedReader bufferedReader = new BufferedReader(read);
					String lineTxt = null;
					while ((lineTxt = bufferedReader.readLine()) != null) {
						String [] a = lineTxt.split("\t");
						countSet.add(a[4]);
					}
					bufferedReader.close();
					read.close();

				}
				System.out.println("countSet size:" + countSet.size());

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public static void procKeyFile(String filePath, int kn) {
		String dir = Configs.readValue("config/track.cfg", "merge.dir");
		FileWriter fw = null;
		HashSet<String> hset = new HashSet<String>();
		try {
			String outfile = dir + "out" + kn + ".bcp";
			fw = new FileWriter(outfile);
			System.out.println("proc file:" + filePath);
			System.out.println("out file:" + outfile);
			File file = new File(filePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file));// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					// System.out.println(lineTxt);
					hset.add(lineTxt);

				}
				procOnekey(hset, fw);
				bufferedReader.close();
				read.close();
			} else {
				System.out.println("找不到指定的文件");
			}
			fw.close();
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}

	}

	public static void merge() {
		String dir = Configs.readValue("config/track.cfg", "key_u.dir");
		File file = new File(dir);
		String[] fileName = file.list();
		int kn = 0;
		for (String strf : fileName) {
			System.out.println(strf);
			if (strf.contains(".key")) {
				procKeyFile(dir + strf, kn);
				kn++;

			}
		}

	}

	public static void main(String[] args) {

		// procDir();
		//readkey();
		//merge();
		countTagValueNum();

	}

}
