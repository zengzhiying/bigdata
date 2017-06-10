package net.zengzhiying;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtils {
    
    private static Configuration conf = null;
    private static FileSystem fs = null;
    private static final String DEFAULT_FS = "hdfs://bigdata:9000";
    
    /**
     * 初始化hadoop hdfs配置
     * 
     */
    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", DEFAULT_FS);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            System.out.println("hdfs配置异常...");
            e.printStackTrace();
        }
    }
    
    /**
     * 程序执行到最后关闭hdfs
     */
    public static void closeFs() {
        if(fs != null) {
            try {
                fs.close();
                System.out.println("关闭FileSystem...success");
            } catch (IOException e) {
                System.out.println("关闭FileSystem异常！");
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 上传本地文件到hdfs
     * @param localFile
     * @param hdfsFile
     */
    public static void uploadFile(String localFile, String hdfsFile) {
        try {
            InputStream in = new FileInputStream(localFile);
            OutputStream out = fs.create(new Path(hdfsFile));
            IOUtils.copyBytes(in, out, conf, true);
            out.close();
            in.close();
        } catch (IOException e) {
            System.out.println("操作hdfs异常...");
            e.printStackTrace();
        }
        
    }
    
    /**
     * 创建hdfs path
     * @param hdfsPath
     * @return 成功返回true 失败返回false
     */
    public static boolean createPath(String hdfsPath) {
        try {
            boolean status = fs.mkdirs(new Path(hdfsPath));
            return status;
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("创建hdfs目录失败...");
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 下载hdfs文件到本地
     * @param hdfsFile
     * @param localFile
     */
    public static void downloadFile(String hdfsFile, String localFile) {
        try {
            InputStream in = fs.open(new Path(hdfsFile));
            OutputStream out = new FileOutputStream(localFile);
            IOUtils.copyBytes(in, out, conf, true);
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("读取hdfs文件失败...");
            e.printStackTrace();
        }
        
    }
    
    /**
     * 删除hdfs文件或目录 如果是目录则递归删除
     * @param hdfsFile
     * @return
     */
    public static boolean deleteFile(String hdfsFile) {
        try {
            // 第二个参数是true表示递归删除目录 默认为false 如果目录下有文件会删除失败
            boolean flag = fs.delete(new Path(hdfsFile), true);
            return flag;
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("删除失败...");
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 重命名hdfs文件 
     * @param hdfsFile 源文件
     * @param newHdfsFile 新文件
     * @return 成功返回true 失败或异常返回false
     */
    public static boolean renameFile(String hdfsFile, String newHdfsFile) {
        try {
            boolean flag = fs.rename(new Path(hdfsFile), new Path(newHdfsFile));
            return flag;
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("重命名文件异常...");
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 检查hdfs中某个文件是否存在
     * @param hdfsFile
     * @return 存在返回true 不存在或异常返回false
     */
    public static boolean checkFile(String hdfsFile) {
        try {
            boolean flag = fs.exists(new Path(hdfsFile));
            return flag;
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("检查文件异常...");
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 在hdfs上直接创建文件 并写入相应的内容
     * @param hdfsFile
     * @param text
     */
    public static void createFile(String hdfsFile, String text) {
        byte[] buff = text.getBytes();
        Path path = new Path(hdfsFile);
        try {
            FSDataOutputStream outputStream = fs.create(path);
            outputStream.write(buff, 0, buff.length);
            outputStream.flush();
            outputStream.close();
            System.out.println("创建文件：" + hdfsFile + "成功...");
        } catch (IOException e) {
            System.out.println("创建hdfs文件失败...");
            e.printStackTrace();
        }
    }
    
    /**
     * 获取hdfs某一路径下的文件列表 单层非递归
     * @param hdfsPath
     */
    public static void getFileList(String hdfsPath) {
        try {
            FileStatus[] fileStatus = fs.listStatus(new Path(hdfsPath));
            for(FileStatus file:fileStatus) {
                System.out.println(file.getPath().toString());
            }
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("获取目录列表异常...");
            e.printStackTrace();
        }
        
    }
    
    /**
     * 获取hdfs某一路径下的文件列表 如果遇到目录会继续递归读取
     * @param hdfsPath
     */
    public static void getFileLists(String hdfsPath) {
        try {
            FileStatus[] fileStatus = fs.listStatus(new Path(hdfsPath));
            for(FileStatus file:fileStatus) {
                if(file.isDirectory()) {
                    getFileLists(file.getPath().toString());
                } else {
                    System.out.println(file.getPath().toString());
                }
            }
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("获取目录列表异常...");
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        uploadFile("/root/test.xml", "/zzylsl/test.xml");
        uploadFile("/root/test.txt", "/zzylsl/test.txt");
//        if(createPath("/zengzhiying")) {
//            System.out.println("创建目录成功...");
//        }
//        downloadFile("/zzylsl/test.txt", "/root/test.txt");
//        System.out.println("下载文件完毕...");
//        if(deleteFile("/zzylsl/test.txt")) {
//            System.out.println("删除文件成功....");
//        }
//        if(renameFile("/zzylsl/test.xml", "/zzylsl/test1.xml")) {
//            System.out.println("重命名文件成功...");
//        }
//        if(!checkFile("/zzylsl/test.xml")) {
//            System.out.println("原文件不存在...");
//        }
//        createFile("/zengzhiying/abc.log", "abc.log");
//        System.out.println("写入文件完毕...");
        getFileList("/");
        System.out.println("遍历...");
        getFileLists("/");
    }
    
}
