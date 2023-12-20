package com.monchickey.examples.sourcesink;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class LabelCount {
    public long count;
    public Timestamp start;
    public Timestamp end;

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "LabelCount{" +
                "count=" + count +
                ", start=" + sdf.format(start) +
                ", end=" + sdf.format(end) +
                '}';
    }
}
