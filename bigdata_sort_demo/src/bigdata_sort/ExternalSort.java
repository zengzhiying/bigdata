package bigdata_sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 外部排序 用在大数据量内存不够用的场合 利用外部存储做中转进行排序
 * 参考自文章 http://mp.weixin.qq.com/s/I7M6moYbVd8XycuBjidYPw
 * @author monchickey
 *
 */

public class ExternalSort {
    public static void main(String[] args) {
        System.out.println("外部排序 start.");
        long start = System.currentTimeMillis();
        perform("bigdata.log", "bigdata.log1", 60000);
        long end = System.currentTimeMillis();
        long subTime = end - start;
        System.out.println("执行耗时: " + subTime + " ms");
    }
    
    public static void perform(String fileName, String outFileName, int bufferedSize) {
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            // 依次读取大文件到缓冲
            System.out.println("开始读取大文件到缓冲.");
            fileReader = new FileReader(fileName);
            bufferedReader =  new BufferedReader(fileReader, 4096);
            int num = 0;
            while(true) {
                boolean flag = false;
                int[] bigdata = new int[bufferedSize];
                int i;
                for(i = 0; i < bufferedSize;i++) {
                    String line = bufferedReader.readLine();
                    if(line == null) {
                        flag = true;
                        break;
                    }
                    bigdata[i] = Integer.valueOf(line);
                }
                System.out.println("读取" + (num + 1) + "个.");
                // 最后一行为空直接退出
                if(i == 0) {
                    break;
                }
                // 做一次选择排序
                GeneralSort.simpleSelectionSort(bigdata);
                // 写入文件
                createSortFile(fileName + ".sort" + num, bigdata);
                if(flag) {
                    break;
                }
                num++;
            }
            // 打开要写入的大文件
            System.out.println("开始排序小文件.");
            fileWriter = new FileWriter(outFileName);
            bufferedWriter = new BufferedWriter(fileWriter);
            // 同时打开num个小文件集合排序
            FileReaderManager[] fileManagers = new FileReaderManager[num + 1];
            for(int i = 0; i <= num; i++) {
                fileManagers[i] = new FileReaderManager(fileName + ".sort" + i);
            }
            for(;;) {
                int[] smallSort = new int[num + 1];
                for(int j = 0; j <= num; j++) {
                    String smallLine = fileManagers[j].getFileLine();
                    if(smallLine != null && !smallLine.equals("") && !smallLine.equals("\n")) {
                        
                        smallSort[j] = Integer.valueOf(smallLine);
                        continue;
                    }
                    smallSort[j] = 0;
                }
//                for(int m = 0; m <= num; m++) {
//                    System.out.print(smallSort[m] + " ");
//                }
//                System.out.println("");
                if(isEffectiveArray(smallSort)) {
                    break;
                }
                
                int[] minNums = arrayMinimum(smallSort);
                // 处理小文件
                fileManagers[minNums[0]].close();
                fileManagers[minNums[0]] = new FileReaderManager(fileName + ".sort" + minNums[0]);
                String[] contents = fileManagers[minNums[0]].getFileContent();
//                System.out.println(contents);
//                System.out.println(minNums[0]);
                if(contents != null) {
                    fileManagers[minNums[0]].close();
                    FileWriterManager fwm = new FileWriterManager(fileName + ".sort" + minNums[0]);
                    fwm.setFileContent(removeIndex0(contents));
                    fwm.close();
                    fileManagers[minNums[0]] = new FileReaderManager(fileName + ".sort" + minNums[0]);
                }
                
                // 写入大文件
                bufferedWriter.write(String.valueOf(minNums[1]));
                bufferedWriter.newLine();
                // 重开小文件
                for(int m = 0; m <= num; m++) {
                    fileManagers[m].close();
                    fileManagers[m] = new FileReaderManager(fileName + ".sort" + m);
                }
            }
            // 依次关闭小文件
            for(int i = 0; i <= num; i++) {
                fileManagers[i].close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭输入输出大文件资源
            try {
                if(bufferedReader != null) {
                    bufferedReader.close();
                }
                if(fileReader != null) {
                    fileReader.close();
                }
                if(bufferedWriter != null) {
                    bufferedWriter.close();
                }
                if(fileWriter != null) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    private static void createSortFile(String fileName, int[] sortNumbers) {
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            fileWriter = new FileWriter(fileName);
            bufferedWriter = new BufferedWriter(fileWriter);
            for(int i = 0; i < sortNumbers.length; i++) {
                if(sortNumbers[i] == 0) {
                    continue;
                }
                bufferedWriter.write(String.valueOf(sortNumbers[i]));
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(bufferedWriter != null) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
    }
    
    private static int[] arrayMinimum(int[] numbers) {
        if(numbers.length == 1) {
            return new int[]{0, numbers[0]};
        } else if(numbers.length > 1) {
            int minimum = numbers[0];
            int index = 0;
            for(int i = 1; i < numbers.length; i++) {
                if(numbers[i] == 0) {
                    continue;
                }
                if(minimum == 0) {
                    minimum = numbers[i];
                    index = i;
                }
                if(numbers[i] < minimum) {
                    minimum = numbers[i];
                    index = i;
                }
            }
            return new int[] {index, minimum};
        } else {
            return new int[] {0, 0};
        }
    }
    
    private static String[] removeIndex0(String[] contents) {
        if(contents.length == 1) {
            return new String[] {""};
        } else if(contents.length > 1) {
            String[] lines = new String[contents.length - 1];
            for(int i = 1; i < contents.length; i++) {
                lines[i-1] = contents[i];
            }
            return lines;
        } else {
            return new String[] {""};
        }
    }
    
    private static boolean isEffectiveArray(int[] numbers) {
        for(int i = 0; i < numbers.length; i++) {
            if(numbers[i] != 0) {
                return false;
            }
        }
        return true;
    }
    
    
}
