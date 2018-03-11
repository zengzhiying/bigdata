package net.zengzhiying;

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

/**
 * Hbase操作工具类
 * @author Administrator
 *
 */

public class HbaseUtils {
    private static Configuration configuration;
    private static Connection connection = null;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "shandong1,shandong2,shandong3,shandong4,shandong5");
        configuration.set("hbase.master", "shandong1");
        
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            System.out.println("建立hbase connection失败！");
            e.printStackTrace();
        }
    }
    
    /**
     * 添加一个value
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     * @return
     */
    public static boolean addValue(String tableName, String rowKey, String columnFamily, String columnName, String columnValue) {
        if(connection == null) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                System.out.println("建立hbase 连接失败！");
                e.printStackTrace();
                return false;
            }
        }
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            System.out.println("获取表失败！");
            e.printStackTrace();
            return false;
        }
        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), columnName.getBytes(), columnValue.getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            System.out.println("添加hbase数据异常！");
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    /**
     * 添加单条row key数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnValues
     * @return
     */
    public static boolean addInfo(String tableName, String rowKey, String columnFamily, Map<String, Object> columnValues) {
        if(connection == null) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                System.out.println("建立hbase 连接失败！");
                e.printStackTrace();
                return false;
            }
        }
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            System.out.println("获取表失败！");
            e.printStackTrace();
            return false;
        }
        Put put = new Put(rowKey.getBytes());
        Set<String> columns = columnValues.keySet();
        for(String columnName:columns) {
            if(columnValues.get(columnName) instanceof java.lang.Integer) {
                System.out.println("整型");
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Integer) columnValues.get(columnName)));
            } else if(columnValues.get(columnName) instanceof java.lang.Long) {
                System.out.println("长整型.");
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((Long) columnValues.get(columnName)));
            } else if(columnValues.get(columnName) instanceof java.lang.String) {
                System.out.println("字符串.");
                put.addColumn(columnFamily.getBytes(), columnName.getBytes(), Bytes.toBytes((String) columnValues.get(columnName)));
            } else {
                System.out.println("类型未知...");
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            System.out.println("添加hbase数据异常！");
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
