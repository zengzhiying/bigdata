package bigdata_sort;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileReaderManager {
    
    private FileReader fileReader = null;
    private BufferedReader bufferedReader = null;
    
    public static void main(String[] args) throws InterruptedException {
        FileReaderManager fm = new FileReaderManager("test.log");
        String[] s = fm.getFileContent();
        System.out.println(s);
        System.out.println(s == null);
        // System.out.println(s.length);
        fm.close();
        System.exit(0);
//        for(String ss:s) {
//            System.out.println(ss);
//        }
        s[2] = "aaaacxxxxxxx111";
        FileWriterManager fwm = new FileWriterManager("test.log");
        fwm.setFileContent(s);
        fwm.close();
        fm.close();
        fm = new FileReaderManager("test.log");
        String[] scscs = fm.getFileContent();
        System.out.println(scscs[2]);
        fm.close();
    }
    
    public FileReaderManager(String fileName) {
        try {
            fileReader = new FileReader(fileName);
            bufferedReader = new BufferedReader(fileReader, 4096);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public String getFileLine() {
        try {
            String line = bufferedReader.readLine();
            return line;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public String[] getFileContent() {
        StringBuffer content = new StringBuffer("");
        try {
            String line = bufferedReader.readLine();
            while(line != null) {
                content.append(line);
                content.append("\n");
                line = bufferedReader.readLine();
            }
            if(!content.toString().equals("")) {
                return content.toString().split("\n");
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public void close() {
        if(bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(fileReader != null) {
            try {
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
}
