package net.zengzhiying.service;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class DataWriteFileService {
	
	/**
	 * 写文件
	 */
	public static void FileWrite(String filename, String text) {
		
		try {
			FileOutputStream fos = new FileOutputStream(filename);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			char[] ch = text.toCharArray();
			for(int i = 0;i < ch.length;i++) {
				osw.write(ch[i]);
				osw.flush();
			}
			osw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
