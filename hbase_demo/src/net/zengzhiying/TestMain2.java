package net.zengzhiying;

import java.util.ArrayList;
import java.util.List;

public class TestMain2 {
    public static void main(String[] args) {
        System.out.println("test main 2");
        List<TestBean> tblist1 = new ArrayList<TestBean>();
        List<String> l1 = new ArrayList<String>();
        l1.add("lkkk");
        List<String> l2 = new ArrayList<String>();
        l1.add("lkkk1");
        TestBean tb1 = new TestBean(3,"sc", l1);
        TestBean tb2 = new TestBean(5, "ckdhv", l2);
        tblist1.add(tb1);
        tblist1.add(tb2);
        List<TestBean> tblist2 = new ArrayList<TestBean>();
        for(TestBean tb:tblist1) {
            tblist2.add(tb);
        }
        tblist2.add(new TestBean(6, "skchs", l2));
        System.out.println(tblist1.size());
        System.out.println(tblist2.size());
        tblist2.clear();
        System.out.println(tblist2.size());
        System.out.println(tblist1.size());
        
    }
}
