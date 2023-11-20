package model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetUrlParam {
	
	public static String getParam(String url) {
		Pattern p = Pattern.compile("^/detail/[0-9]+$");
		Pattern p1 = Pattern.compile("[0-9]{1,}");
		Matcher m = p.matcher(url);
		Matcher m1 = p1.matcher(url);
		StringBuffer buffer = new StringBuffer();
		if(m.matches()) {
			//匹配成功,抽取数据
			
			while(m1.find()) {
				buffer.append(m1.group());
			}
			
			return buffer.toString();
		} else {
			//匹配失败
			return "error";
		}
		
	}
	
	public static void main(String[] args) {
		
		String url = getParam("/detail/23");
		System.out.println(url);
		
	}
	
}
