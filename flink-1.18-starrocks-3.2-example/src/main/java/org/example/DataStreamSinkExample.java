package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Base64;


/**
 * StarRocks table
 * CREATE TABLE test.user (
 *     id int(11) NOT NULL COMMENT "",
 *     name varchar(65533) NULL DEFAULT "" COMMENT "",
 *     score smallint NOT NULL DEFAULT "0" COMMENT "",
 *     vector binary NOT NULL
 * )
 * ENGINE=OLAP
 * DUPLICATE KEY(id)
 * DISTRIBUTED BY HASH(id);
 */
public class DataStreamSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-example")
                .setGroupId("input-example-group")
                .setProperty("enable.auto.commit", "true")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "http://127.0.0.1:8030")
                .withProperty("database-name", "test")
                .withProperty("table-name", "user")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.semantic", "at-least-once")  // 默认语义 at-least-once
                // 下面两个参数只有在 at-least-once 时才生效，exactly-once 下由 checkpoint 触发
                .withProperty("sink.buffer-flush.max-bytes", "134217728")  // 写入最大缓冲大小，默认是 90M
                .withProperty("sink.buffer-flush.interval-ms", "5000") // 写入间隔时间，默认是 300s
                .build();

        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.INT().notNull())
                .field("name", DataTypes.STRING())
                .field("score", DataTypes.SMALLINT())
                // StarRocks connector 目前不支持二进制字段
                // .field("vector", DataTypes.BYTES())
                .build();

        UserRowTransformer transformer = new UserRowTransformer();
        SinkFunction<User> sink = StarRocksSink.sink(schema, options, transformer);

        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        sourceStream.print();

        sourceStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String s) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(s, User.class);
            }
        }).filter(user -> user.getId() > 0 && !"".equals(user.getName()))
                .flatMap(new FlatMapFunction<User, User>() {
                    @Override
                    public void flatMap(User user, Collector<User> collector) throws Exception {
                        if("".equals(user.getVector())) {
                            user.setVectorBytes(new byte[0]);
                        } else {
                            byte[] vectorBytes = Base64.getDecoder().decode(user.getVector());
                            user.setVectorBytes(vectorBytes);
                        }
                        collector.collect(user);
                    }
                })
                .addSink(sink);

        env.execute("Flink StarRocks Example");

    }
}