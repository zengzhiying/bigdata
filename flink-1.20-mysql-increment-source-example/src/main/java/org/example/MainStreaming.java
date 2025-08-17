package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.example.source.MySqlIncrementSource;

/**
 * 示例的 MySQL 建表语句：
 * CREATE TABLE user (
 *     id INT AUTO_INCREMENT PRIMARY KEY,
 *     name VARCHAR(255),
 *     age INT
 * );
 */
public class MainStreaming {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> sourceStream = env.fromSource(new MySqlIncrementSource("jdbc:mysql://192.168.11.28:9030/test", "root", "doris-123456", "user"),
                WatermarkStrategy.noWatermarks(), "MySQL Increment Source").setParallelism(4);
        sourceStream.print();

        env.execute("MySQL Increment custom source example");
    }
}
