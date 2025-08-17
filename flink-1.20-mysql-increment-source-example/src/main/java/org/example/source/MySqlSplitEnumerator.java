package org.example.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MySqlSplitEnumerator implements SplitEnumerator<MySqlSplit,Long> {
    private final static Logger log = LoggerFactory.getLogger(MySqlSplitEnumerator.class);

    private final SplitEnumeratorContext<MySqlSplit> enumeratorContext;
    private long nextStartId = 1;
    private final String jdbcUrl, username, password;
    private final String table;

    public MySqlSplitEnumerator(SplitEnumeratorContext<MySqlSplit> context, String jdbcUrl,
                                String username, String password, String table) throws ClassNotFoundException {
        enumeratorContext = context;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.table = table;
        Class.forName("com.mysql.cj.jdbc.Driver");
    }

    private long getMaxIdFromTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("SELECT MAX(id) FROM %s", table))) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private List<MySqlSplit> discoverSplits() throws Exception {
        long SPLIT_BATCH_SIZE = 1000;
        long maxId;
        try {
            maxId = getMaxIdFromTable();
        } catch (Exception e) {
            log.error("getMaxIdFromTable error: {}", e.getMessage());
            return Collections.emptyList();
        }

        List<MySqlSplit> splits = new ArrayList<>();
        while(nextStartId <= maxId) {
            long endId = Math.min(nextStartId + SPLIT_BATCH_SIZE, maxId + 1);
            String splitId = "split-" + nextStartId + "-" + endId;
            log.info("discover split id {}", splitId);
            splits.add(new MySqlSplit(splitId, nextStartId, endId));
            nextStartId = endId;
        }
        return splits;
    }

    @Override
    public void start() {
        enumeratorContext.callAsync(this::discoverSplits, (splits, throwable) -> {
            // 如果 discoverSplits 方法抛出异常，则这里可能会接收到 null
            if(splits == null) {
                log.error("splits is null.");
                return;
            }
            for(MySqlSplit split : splits) {
                int subTaskId = (split.splitId().hashCode() & Integer.MAX_VALUE) % enumeratorContext.currentParallelism();
                log.info("MySQL Split Enumerator assign split {} to subTaskId {}", split.splitId(), subTaskId);
                enumeratorContext.assignSplit(split, subTaskId);
            }
        }, 5000, 10000);
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {

    }

    @Override
    public void addSplitsBack(List<MySqlSplit> list, int i) {

    }

    @Override
    public void addReader(int i) {

    }

    @Override
    public Long snapshotState(long l) throws Exception {
        return nextStartId;
    }

    @Override
    public void close() throws IOException {

    }
}
