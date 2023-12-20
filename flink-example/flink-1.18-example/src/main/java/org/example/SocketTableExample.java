package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SocketTableExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 有界流可以设置 BATCH 和 STREAMING，但是无界流只能设置 STREAMING
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE user_score (name STRING, score INT)\n" +
                "WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '9999',\n" +
                "  'byte-delimiter' = '10',\n" +
                "  'format' = 'changelog-csv',\n" +
                "  'changelog-csv.column-delimiter' = '|'\n" +
                ");");

        tableEnv.executeSql("SELECT * FROM user_score").print();
        // 无界流的聚合是实时更新的，有界流的聚合结果必须等 Source 退出后才可以得到
        tableEnv.executeSql("SELECT count(*) FROM user_score").print();
    }
}