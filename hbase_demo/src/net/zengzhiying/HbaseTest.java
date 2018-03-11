package net.zengzhiying;

import java.util.HashMap;
import java.util.Map;

public class HbaseTest {
    public static void main(String[] args) {
        System.out.println(HbaseUtils.addValue("vehicles", "test1", "info", "image", "ssss"));
        Map<String, Object> aaa = new HashMap<String, Object>();
        aaa.put("name1", "kk");
        aaa.put("name2", 201523929288292L);
        aaa.put("name3", 23);
        System.out.println(HbaseUtils.addInfo("vehicles", "test2", "info", aaa));
    }
}
