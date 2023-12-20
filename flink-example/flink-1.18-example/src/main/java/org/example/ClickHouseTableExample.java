package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ClickHouseTableExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**
         * ClickHouse Table
         *
         * CREATE TABLE user_score
         * (
         *     `name` String,
         *     `score` Int32,
         *     `user_id` FixedString(16)
         * )
         * ENGINE = MergeTree
         * ORDER BY user_id
         *
         * Flink Table
         *
         * CREATE TABLE user_score (name STRING, score INT, user_id BYTES)
         * WITH (
         *   'connector' = 'clickhouse',
         *   'hostname' = 'localhost',
         *   'port' = '8123',
         *   'username' = 'default',
         *   'password' = '',
         *   'database' = 'default',
         *   'table' = 'user_score',
         *   'format' = 'clickhouse-row'
         * );
         */
        tableEnv.executeSql("CREATE TABLE user_score (\n" +
                "    `name` STRING,\n" +
                "    `score` INTEGER,\n" +
                "    `user_id` BYTES\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '8123',\n" +
                "    'username' = 'default',\n" +
                "    'password' = 'wSqDxDAt',\n" +
                "    'database' = 'default',\n" +
                "    'table' = 'user_score',\n" +
                "    'format' = 'clickhouse-row'\n" +
                ");\n").print();

        tableEnv.executeSql("SELECT sum(score), name from user_score group by name;").print();
    }
}
