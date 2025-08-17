package org.example.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class MySqlSourceReader implements SourceReader<RowData, MySqlSplit> {
    private static final Logger log = LoggerFactory.getLogger(MySqlSourceReader.class);

    private final String jdbcUrl, user, password, table;

    private final Queue<MySqlSplit> splits = new ArrayDeque<>();

    private CompletableFuture<Void> available = new CompletableFuture<>();

    public MySqlSourceReader(String jdbcUrl, String user, String password, String table) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.table = table;
    }

    @Override
    public void start() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> readerOutput) throws Exception {
        MySqlSplit split = splits.poll();
        if(split == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            String sql = String.format("SELECT id, name, age FROM %s WHERE id >= ? AND id < ?", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                log.info("polled splitId {}", split.splitId());
                ps.setLong(1, split.getStartId());
                ps.setLong(2, split.getEndId());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // 可用自定义 RowData 序列化或 POJO
                        RowData row = convertToRowData(rs);
                        readerOutput.collect(row);
                    }
                }
            }
        }
        return splits.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
    }

    private RowData convertToRowData(ResultSet rs) throws SQLException {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, rs.getLong("id"));
        row.setField(1, StringData.fromString(rs.getString("name")));
        row.setField(2, rs.getInt("age"));
        return row;
    }

    @Override
    public List<MySqlSplit> snapshotState(long l) {
        return new ArrayList<>(splits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if(!splits.isEmpty()) return CompletableFuture.completedFuture(null);
        return available;
    }

    @Override
    public void addSplits(List<MySqlSplit> splits) {
        this.splits.addAll(splits);
        available.complete(null);
        available = new CompletableFuture<>();
    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() throws Exception {
    }
}
