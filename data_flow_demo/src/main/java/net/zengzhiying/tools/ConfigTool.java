package net.zengzhiying.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 获取配置文件key对应的value字符串
 * @author zengzhiying
 *
 */

public class ConfigTool {
	
	private static InputStream is = ConfigTool.class.getClassLoader().getResourceAsStream("net/zengzhiying/configs/config.properties");
	private static Properties props = new Properties();
	
	static {
		try {
			props.load(is);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取对应的配置字符串
	 * @param key
	 * @return
	 */
	public static String getConfig(String key) {
		return props.getProperty(key);
	}
	
}