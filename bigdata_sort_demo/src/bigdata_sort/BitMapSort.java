package bigdata_sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.BitSet;

/**
 * 使用位图法排序 排序之后重复数据会移除 一般用于海量数据去重排序
 * @author monchickey
 *
 */

public class BitMapSort {
    private static BitSet bits;
    public static void main(String[] args) {
        System.out.println("bitmap start.");
        long start = System.currentTimeMillis();
        perform("bigdata.log", 306488904, "bigdata1.log", 4096, 4096);
        long stop = System.currentTimeMillis();
        long subTime = stop - start;
        System.out.println("执行耗时: " + subTime + " ms");
    }
    
    public static void perform(String fileName, int total, String outFileName, int readerBufferSize, int writerBufferSize) {
        bits = new BitSet(total);
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            fileReader = new FileReader(fileName);
            bufferedReader =  new BufferedReader(fileReader, readerBufferSize);
            fileWriter = new FileWriter(outFileName);
            bufferedWriter = new BufferedWriter(fileWriter, writerBufferSize);
            String line = "";
            do {
                line = bufferedReader.readLine();
                if(line != null) {
                    bits.set(Integer.valueOf(line));
                }
            } while(line != null);
            for(int i = 0; i < bits.length(); i++) {
                if(bits.get(i)) {
                    line = String.valueOf(i);
                    bufferedWriter.write(line);
                    bufferedWriter.newLine();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                System.out.println("关闭文件.");
                bufferedWriter.close();
                fileWriter.close();
                bufferedReader.close();
                fileReader.close();
            } catch (IOException e) {
                System.out.println("文件句柄关闭异常！");
                e.printStackTrace();
            }
        }
        
    }
    
    /**
     * 初始化方法 默认就是false此方法不调用
     */
    public static void initBits() {
        int length = bits.length();
        for(int index = 0; index < length; index++) {
            bits.set(index, false);
        }
    }
}
