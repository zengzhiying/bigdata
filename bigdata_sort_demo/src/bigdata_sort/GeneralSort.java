package bigdata_sort;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 * 使用一般排序排序大文件 内存直接溢出
 * @author monchickey
 *
 */

public class GeneralSort {
    public static void main(String[] args) {
        int[] bigdata = new int[100];
        FileInputStream inputStream = null;
        Scanner scan = null;
        try {
            inputStream = new FileInputStream("bigdata.log");
            scan = new Scanner(inputStream, "UTF-8");
            int i = 0;
            while(scan.hasNextLine()) {
                String line = scan.nextLine();
//                System.out.println(line);
                bigdata[i] = Integer.valueOf(line);
                i++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            System.out.println("关闭资源.");
            if(scan != null) {
                scan.close();
            }
            if(inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    System.out.println("输入流关闭异常!");
                    e.printStackTrace();
                }
            }
        }
        simpleSelectionSort(bigdata);
        for(int i = 0;i < bigdata.length; i++) {
            System.out.println(bigdata[i]);
        }
    }
    
    
    /**
     * 简单选择排序 - 直接选择排序
     * 对数组a升序排序
     * 排序不稳定 时间复杂度O(n^2)
     * @param a
     * @return
     */
    public static int[] simpleSelectionSort(int[] a) {
        int N = a.length;
        for(int i = 0;i < N - 1;i++) {
            //元素交换
            for(int j = i+1;j < N;j++) {
                if(a[j] < a[i]) {
                    int temp = a[j];
                    a[j] = a[i];
                    a[i] = temp;
                }
            }
        }
        return a;
    }
}
