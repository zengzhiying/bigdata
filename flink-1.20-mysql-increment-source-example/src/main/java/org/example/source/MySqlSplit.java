package org.example.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class MySqlSplit implements SourceSplit {
    private final String splitId;
    private final long startId;
    private final long endId;

    public MySqlSplit(String splitId, long startId, long endId) {
        this.splitId = splitId;
        this.startId = startId;
        this.endId = endId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public long getStartId() {
        return startId;
    }

    public long getEndId() {
        return endId;
    }
}
