package bigdata_sort;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

public class FileWriterManager {
    
    private FileWriter fileWriter = null;
    private BufferedWriter bufferedWriter = null;
    
    public FileWriterManager(String fileName) {
        try {
            fileWriter = new FileWriter(fileName);
            bufferedWriter = new BufferedWriter(fileWriter, 4096);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void setFileContent(String[] fileContent) {
        try {
            for(int i = 0; i < fileContent.length; i++) {
                if(!fileContent[i].equals("")) {
                    bufferedWriter.write(fileContent[i]);
                    bufferedWriter.newLine();
                }
            }
            bufferedWriter.flush();
        } catch (IOException e) {
                e.printStackTrace();
        }
    }
    
    public void close() {
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
