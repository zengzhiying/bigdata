package com.monchickey;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import javax.lang.model.type.UnknownTypeException;

/**
 * HBase 操作工具
 * @author monchickey
 *
 */

public class HbaseUtil {
    private static Configuration configuration;
    private static Connection connection = null;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
        configuration.set("hbase.master", "node1");
        
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 添加一个 column 的 value
     * @param tableName 表名
     * @param rowKey  表rowKey
     * @param columnFamily 列族
     * @param columnName  列名
     * @param columnValue 值
     * @return
     */
    public static boolean addColumn(String tableName, String rowKey, String columnFamily, String columnName, String columnValue) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), columnName.getBytes(), columnValue.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }
    
    /**
     * 添加多列数据
     * @param tableName 表名
     * @param rowKey  写入的rowKey
     * @param columnFamily 列族
     * @param columnValues 一组列和值
     * @return
     */
    public static boolean addColumns(String tableName, String rowKey, String columnFamily, Map<String, Object> columnValues) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        Put put = new Put(rowKey.getBytes());
        Set<String> columns = columnValues.keySet();
        for(String columnName:columns) {
            Object val = columnValues.get(columnName);
            if(val instanceof Integer) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Integer) val));
            } else if(val instanceof Long) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Long) val));
            } else if(val instanceof String) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((String) val));
            } else if(val instanceof Short) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Short) val));
            } else if(val instanceof Float) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Float) val));
            } else if(val instanceof Double) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Double) val));
            } else if(val instanceof Boolean) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Boolean) val));
            } else if(val instanceof byte[]) {
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), (byte[]) val);
            } else {
                new Exception("Unknown type: " + val.getClass().toString() +
                        " key: " + columnName + " value: " + val).printStackTrace();
            }
        }

        try {
            if(!put.isEmpty()) table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }
}
