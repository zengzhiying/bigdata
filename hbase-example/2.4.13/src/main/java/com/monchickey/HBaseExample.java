package com.monchickey;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class HBaseExample {
    public static void main(String[] args) throws IOException, DecoderException {
        Configuration config = HBaseConfiguration.create();
        String path = new HBaseExample().getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();

        System.out.println(path);
        config.addResource(new Path(path));
        try {
            HBaseAdmin.available(config);
            System.out.println("HBase available.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 创建表
        TableName table = TableName.valueOf("empTable");
        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();

        if(!admin.tableExists(table)) {
            TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(table)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf2"))
                    .build();
            admin.createTable(tableDesc);

            System.out.println("Created table: " + table.getNameAsString());
        } else {
            // create 'empTable', {NAME => 'cf1', COMPRESSION => 'LZ4'},{NAME => 'cf2', COMPRESSION => 'LZ4'}, {NUMREGIONS => 16, SPLITALGO => 'UniformSplit'}
            System.out.println("Table: " + table.getNameAsString() + " is exists.");
        }

        // 写入数据
        Table tb = conn.getTable(table);
        Put put = new Put("row1".getBytes());
        put.addColumn("cf1".getBytes(), Bytes.toBytes("a"), Bytes.toBytes("yes"));
        put.addColumn("cf2".getBytes(), Bytes.toBytes("a"), Bytes.toBytes("hello"));
        tb.put(put);

        // 查找一条数据
        Get get = new Get(Bytes.toBytes("row1"));
        Result point = tb.get(get);
        System.out.println(new String(point.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("a"))));
        System.out.println(new String(point.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("a"))));

        // scan
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("a"));
        scan.setFilter(new PrefixFilter(Bytes.toBytes("row")));
        ResultScanner scanner = tb.getScanner(scan);
        for(Result result : scanner) {
            System.out.println("scan: " + result);
        }

        tb.close();

        Random rand = new Random();
        tb = conn.getTable(table);
        for(int n = 0; n < 100; n++) {
            long second1 = System.currentTimeMillis();
            // 批量写入 10万条
            List<Put> puts = new ArrayList<>(100000);
            for(int i = 0; i < 100000; i++) {
                UUID uuid = UUID.randomUUID();
                Put p = new Put(Hex.decodeHex(uuid.toString().replaceAll("-", "")));
                byte[] b = new byte[8];
                rand.nextBytes(b);
                p.addColumn("cf1".getBytes(), Bytes.toBytes("json"), b);
                puts.add(i, p);
            }
            tb.put(puts);
            long second2 = System.currentTimeMillis();
            System.out.println("time: " + (second2 - second1));
        }


        // delete
        Delete delete = new Delete(Bytes.toBytes(29));
//        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("json"))
        tb.delete(delete);

        tb.close();


        admin.close();

        conn.close();

    }
}
