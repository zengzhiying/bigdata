package org.example.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

public class MySqlIncrementSource implements Source<RowData, MySqlSplit, Long> {
    private final String jdbcUrl, username, password, table;

    public MySqlIncrementSource(String jdbcUrl, String username, String password, String table) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.table = table;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<MySqlSplit, Long> createEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext) throws Exception {
        return new MySqlSplitEnumerator(enumContext, jdbcUrl, username, password, table);
    }

    @Override
    public SplitEnumerator<MySqlSplit, Long> restoreEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext, Long checkpoint) throws Exception {
        return new MySqlSplitEnumerator(enumContext, jdbcUrl, username, password, table);
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return new MySqlSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
        return new LongVersionedSerializer();
    }

    @Override
    public SourceReader<RowData, MySqlSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new MySqlSourceReader(jdbcUrl, username, password, table);
    }
}
