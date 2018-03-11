package net.zengzhiying.util;

public class MessageProcessUtil {
	private static String inits = null;
	static {
		if (inits == null) {
			System.out.println("初始化对象..");
			inits = " msg pre";
		} else {
			System.out.println("实例已经被实例化.");
		}
	}

	public static String messageProcess(String msg) {
		return msg + inits;
	}
}
