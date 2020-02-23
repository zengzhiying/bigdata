package com.monchickey.examples.sourcesink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ClickhouseStreamSink extends RichSinkFunction<List<ImageBody>> {
    public static final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String chHost = "127.0.0.1";
    public static final int chPort = 8123;
    public static final String database = "test_db";
    public static final String username = "default";
    public static final String password = "";

    public static final int insertBatch = 10;

    private Connection conn;
    private List<ImageBody> batchImageBodys = new ArrayList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        if(conn == null) {
            System.out.println("Clickhouse连接失败!");
            return;
        }
        System.out.println("Clickhouse连接成功!");
    }

    public Connection getConnection() {
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            System.out.println("驱动加载失败!");
            e.printStackTrace();
            return null;
        }
        String jdbcUrl = String.format("jdbc:clickhouse://%s:%d/%s?user=%s&password=%s",
                chHost, chPort, database, username, password);
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            System.out.println("获取连接失败!");
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(conn != null) {
            conn.close();
            System.out.println("ClickHouse connection closed.");
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public void invoke(List<ImageBody> imageBodys, Context context) throws Exception {
        batchImageBodys.addAll(imageBodys);
        if(batchImageBodys.size() >= insertBatch) {
            try(PreparedStatement ps = conn.prepareStatement("INSERT INTO image_body (image_id, image_body, date) VALUES (?, ?, ?)")) {
                for(ImageBody imageBody: batchImageBodys) {
                    ps.setInt(1, imageBody.imageId);
                    ps.setBytes(2, imageBody.imageBody);
                    Date nowDate = new Date();
                    ps.setDate(3, new java.sql.Date(nowDate.getTime()));

                    ps.addBatch();
                }
                int[] batchCount = ps.executeBatch();
                batchImageBodys.clear();
                System.out.println("成功插入条数: " + batchCount.length);
            } catch (SQLException e) {
                System.out.println("数据库插入失败！");
                e.printStackTrace();
            }
        } else {
            System.out.println("Delay insert.");
        }
    }
}
