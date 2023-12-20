package com.monchickey.examples.sourcesink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;

public class ClickhouseSink extends RichSinkFunction<List<ImageLabel>> {
    public static final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String chHost = "127.0.0.1";
    public static final int chPort = 8123;
    public static final String database = "test_db";
    public static final String username = "default";
    public static final String password = "";

    private Connection conn;

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

    @Override
    public void invoke(List<ImageLabel> imageLabels, Context context) throws Exception {
        try(PreparedStatement ps = conn.prepareStatement("INSERT INTO image_label (label_id, label_name, date) VALUES (?, ?, ?)")) {
            for(ImageLabel imageLabel : imageLabels) {
                ps.setInt(1, imageLabel.labelId);
                ps.setString(2, imageLabel.labelName);
                Date currentDate = new Date();
                ps.setDate(3, new java.sql.Date(currentDate.getTime()));
                ps.addBatch();
            }
            int[] batchCount = ps.executeBatch();
            System.out.println("成功插入条数: " + batchCount.length);
        } catch (SQLException e) {
            System.out.println("数据库插入失败！");
            e.printStackTrace();
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
}
