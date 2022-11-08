package com.monchickey.examples.streamsql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

public class StreamSQLExample {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "flink";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv;
        if(Objects.equals(planner, "blink")) {
            // blink sql引擎
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();
            tableEnv = StreamTableEnvironment.create(env, settings);
        } else if(Objects.equals(planner, "flink")) {
            // flink sql引擎
            tableEnv = StreamTableEnvironment.create(env);
        } else {
            System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
                    "where planner (it is either flink or blink, and the default is flink) indicates whether the " +
                    "example uses flink planner or blink planner.");
            return;
        }

        // 从collection构建DataStream
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)
        ));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)
        ));

        // 转换DataStream为table
        Table tableA = tableEnv.fromDataStream(orderA, "user, product, amount");
        // 第二种方式, 注册DataStream到table, OrderB为表名称
        tableEnv.registerDataStream("OrderB", orderB, "user, product, amount");

        // union两个表
        Table result = tableEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 2");

        tableEnv.toAppendStream(result, Order.class).print();

        env.execute("Stream SQL Example.");
    }


    /**
     * 用户自定义数据类
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}
