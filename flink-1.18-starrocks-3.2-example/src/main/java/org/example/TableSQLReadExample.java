package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableSQLReadExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StarRocks 只支持 BATCH 模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // Table API 不支持筛选列
        tableEnv.executeSql("CREATE TABLE flink_test\n" +
                "(\n" +
                "    `id` INT,\n" +
                "    `name` STRING,\n" +
                "    `score` SMALLINT\n" +
                ")\n" +
                "WITH\n" +
                "(\n" +
                "    'connector'='starrocks',\n" +
                "    'scan-url'='127.0.0.1:8030',\n" +
                "    'jdbc-url'='jdbc:mysql://127.0.0.1:9030',\n" +
                "    'username'='root',\n" +
                "    'password'='',\n" +
                "    'database-name'='test',\n" +
                "    'table-name'='user'\n" +
                ");");

        tableEnv.executeSql("SELECT id, name FROM flink_test WHERE score > 90 ORDER BY id ASC;").print();
        Table flinkTable = tableEnv.sqlQuery("SELECT id, name, score FROM flink_test WHERE score > 90;");
        flinkTable.printSchema();

        // tableEnv toDataStream/toChangelogStream 的时候要注意
        // 因为 Table API 不支持过滤列，因此转换为流处理时所有列必须和表中的列完全一致
        // 否则可能会报错：ArrayIndexOutOfBoundsException，返回结果放置时会放置所有的值，所以会越界
        // 如果后面存在二进制的字段，但是当前的 Table API 不支持，这时候会报错：Failed to open scanner.INVALID_ARGUMENT[Unknown logical type(46)]
        // 因此表中如果存在多余列需提前删除：ALTER TABLE user DROP COLUMN vector;
        tableEnv.toDataStream(flinkTable)
                .map((MapFunction<Row, User>) row -> {
            User u = new User();
            u.setId(row.<Integer>getFieldAs("id"));
            u.setName(row.getFieldAs("name"));
            u.setScore(row.<Short>getFieldAs("score"));
            return u;
        }).print();

//        DataStream<Row> tableStream = tableEnv.toDataStream(flinkTable);
//        tableStream.print();
        // 运行后面 toDataStream 后的作业
        env.execute("Table To DataStream.");

    }
}
