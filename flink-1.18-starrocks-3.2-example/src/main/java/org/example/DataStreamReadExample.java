package org.example;

import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;

public class DataStreamReadExample {
    public static void main(String[] args) throws Exception {
        // 测试前在原表添加字段：ALTER TABLE user ADD COLUMN read_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;
        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("scan-url", "127.0.0.1:8030")
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("table-name", "user")
                .withProperty("database-name", "test")
                // 读取的列和表中所有列不一致时一定要指定具体的列
                // 否则可能会报错：ArrayIndexOutOfBoundsException，返回结果放置时会放置所有的值，所以会越界
                // 如果后面存在二进制的字段，但是当前的 source 不支持，这时候会报错：Failed to open scanner.INVALID_ARGUMENT[Unknown logical type(46)]
                .withProperty("scan.columns", "id,name,score,read_time")
                // 可以指定下推到 StarRocks 的条件，避免全表拉取
                .withProperty("scan.filter", "id > 1")
                .build();
        TableSchema tableSchema = TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("score", DataTypes.SMALLINT())
                .field("read_time", DataTypes.TIMESTAMP())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> source = env.addSource(StarRocksSource.source(tableSchema, options));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TypeInformation<?>[] retTypes = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.SHORT_TYPE_INFO,
                TypeInformation.of(TimestampData.class)
        };

        SingleOutputStreamOperator<Row> sourceStream = source
                .map(rowData -> Row.of(rowData.getInt(0),
                        rowData.getString(1).toString(), rowData.getShort(2),
                        // 类型要和 retTypes 中定义的一致
                        rowData.getTimestamp(3, 6)))
                // 若不设置类型后面使用时可能会报错: Unable to find a field named 'column name' 等其他的错误
                .returns(new RowTypeInfo(retTypes, new String[]{"id", "name", "score", "read_time"}));

        Table sourceTable = tableEnv.fromDataStream(sourceStream);
                // 这个是在客户端执行，所以提交到集群不生效，会报错：Aliasing more fields than we actually have.
//                .as("id", "name", "score");

        sourceTable.printSchema();

        tableEnv.createTemporaryView("sourceTable", sourceTable);
        Table tableRes = tableEnv.sqlQuery("SELECT id, name, score, read_time FROM sourceTable WHERE score > 90 AND name='bob';");
        // ROW 类型打印会有 EOFException，如果打印需要转换为常用的类型，打印时相当于提交批计算任务
//        tableRes.execute().print();
        DataStream<Row> tableStream = tableEnv.toDataStream(tableRes);
        tableStream.map(row -> {
            User u = new User();
            u.setId(row.<Integer>getFieldAs("id"));
            u.setName(row.getFieldAs("name"));
            u.setScore(row.<Short>getFieldAs("score"));
            System.out.println(row.<TimestampData>getFieldAs("read_time"));
            return u;
        }).print();

        source.print();
        source.map((MapFunction<RowData, User>) rowData -> {
            User u = new User();
            u.setId(rowData.getInt(0));
            u.setName(rowData.getString(1).toString());
            u.setScore(rowData.getShort(2));
            return u;
        }).print();
        env.execute("StarRocks Stream source");
    }
}
