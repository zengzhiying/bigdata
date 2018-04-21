package bigdata_sort;

import java.util.Random;

import coodog.fileprocess.FileIO;


/**
 * 生成5亿个整数
 * @author Administrator
 *
 */

public class BigdataCreate {
    public static void main(String[] args) {
        Random random = new Random();
        FileIO fi = new FileIO();
        for(int i = 0;i < 1000000; i++) {
            fi.fileAppend("bigdata.log", random.nextInt(3000000) + "\n");
            // 使用重定向来生成文件
            // System.out.println(random.nextInt(3000000));
        }
    }
}
