package com.monchickey.sink;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionTrace {
    public static String getStackTraceAsString(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter, true);
        e.printStackTrace(printWriter);
        stringWriter.flush();
        printWriter.close();
        return stringWriter.toString();
    }
}
