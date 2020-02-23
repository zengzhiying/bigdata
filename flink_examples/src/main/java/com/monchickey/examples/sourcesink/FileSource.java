package com.monchickey.examples.sourcesink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class FileSource extends RichSourceFunction<String> {
    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private static final String fileName = "/opt/word_source.txt";

    InputStreamReader inputReader = null;
    BufferedReader bf = null;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        System.out.println("FileSource run开始. ");
        if(bf == null || inputReader == null) {
            System.out.println("文件句柄为空!");
            return;
        }
        String line;
        while(isRunning && (line = bf.readLine()) != null) {
            sourceContext.collect(line);
            Thread.sleep(500L);
        }
        System.out.println("FileSource run执行完毕. ");
        Thread.sleep(5000L);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("FileSource Close.");
        if(bf != null) {
            bf.close();
        }
        if(inputReader != null) {
            inputReader.close();
        }
        System.out.println("FileSource Closed.");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("FileSource Open.");
        File file = new File(fileName);
        inputReader = new InputStreamReader(new FileInputStream(file));
        bf = new BufferedReader(inputReader);
        System.out.println("FileSource Opened.");
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }
}
