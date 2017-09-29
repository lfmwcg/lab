package trackProc;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.Properties;

public class Configs {

	// 根据key读取value
	public static String readValue(String filePath, String key) {
		Properties props = new Properties();
		try {

			InputStream in = new BufferedInputStream(new FileInputStream(filePath));
			props.load(new InputStreamReader(in,"UTF-8"));
			String value = props.getProperty(key);
			System.out.println(key +"="+ value);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// 读取properties的全部信息
	public static void readProperties(String filePath) {
		Properties props = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(filePath));
			props.load(new InputStreamReader(in,"UTF-8"));
			Enumeration en = props.propertyNames();
			while (en.hasMoreElements()) {
				String key = (String) en.nextElement();
				String Property = props.getProperty(key);
				System.out.println(key +"="+ Property);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 写入properties信息
	public static void writeProperties(String filePath, String parameterName, String parameterValue) {
		Properties prop = new Properties();
		try {
			InputStream fis = new FileInputStream(filePath);
			// 从输入流中读取属性列表（键和元素对）
			prop.load(new InputStreamReader(fis,"UTF-8"));
			OutputStream fos = new FileOutputStream(filePath);
			prop.setProperty(parameterName, parameterValue);
			// 以适合使用 load 方法加载到 Properties 表中的格式，
			// 将此 Properties 表中的属性列表（键和元素对）写入输出流
			prop.store(new OutputStreamWriter(fos, "utf-8"), "Update '" + parameterName + "' value");
		} catch (IOException e) {
			System.err.println("Visit " + filePath + " for updating " + parameterName + " value error");
		}
	}

	public static void main(String[] args) {
		String cfgfile = "config/tsdb.cfg";
		readValue(cfgfile, "data.dir");
		writeProperties(cfgfile, "age", "39");
		writeProperties(cfgfile, "name", "李伟");
		readProperties(cfgfile);
		System.out.println("OK");
	}
}