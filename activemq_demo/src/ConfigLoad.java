

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigLoad {
	
	public static String getConfig(String key) {
		Properties pro = new Properties();
		String configValue = null;
		try {
			FileInputStream in = new FileInputStream("./configs.properties");
			pro.load(in);
			configValue = pro.getProperty(key);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return configValue;
	}
	
	public static void main(String[] args) {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String configVale = getConfig("now_date");
		System.out.println(configVale);
	}
	
}
