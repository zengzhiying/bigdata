package org.example;

import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Base64;
import java.util.Properties;

public class BinaryArrayExample {

    /**
     * <a href="https://www.javatips.net/api/tinker-master/third-party/aosp-dexutils/src/main/java/com/tencent/tinker/android/dex/Leb128.java">Android LEB128 代码</a>
     * <a href="https://en.wikipedia.org/wiki/LEB128">维基百科</a>
     * <a href="https://clickhouse.com/docs/en/interfaces/formats/#rowbinary">ClickHouse RowBinary 格式说明</a>
     * @param out 输出的 LEB128 字节流
     * @param value 输入待编码的整数
     * @return 写入的字节个数
     */
    public static int writeUnsignedLeb128(ByteArrayOutputStream out, int value) {
        int remaining = value >>> 7;
        int bytesWritten = 0;
        while (remaining != 0) {
            out.write((byte) ((value & 0x7f) | 0x80));
            ++bytesWritten;
            value = remaining;
            remaining >>>= 7;
        }

        out.write((byte) (value & 0x7f));
        ++bytesWritten;

        return bytesWritten;
    }
    public static void main(String[] args) throws SQLException, IOException {

        String url = "jdbc:ch:http://172.20.231.26:8123/";
        Properties prop = new Properties();
        prop.setProperty("user", "default");
        prop.setProperty("password", "123456");
        prop.setProperty("client_name", "agent 1");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, prop);

        Connection conn = dataSource.getConnection();
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("CREATE DATABASE IF NOT EXISTS test");
        resultSet.close();
//         resultSet = statement.executeQuery("DROP TABLE IF EXISTS test.tb_arr; CREATE TABLE test.tb_arr" +
//                " (id UInt32, name String, vectors Array(String)) ENGINE=MergeTree() ORDER BY id");
        resultSet = statement.executeQuery("CREATE TABLE IF NOT EXISTS test.tb_arr" +
                " (id UInt32, name String, vectors Array(String)) ENGINE=MergeTree() ORDER BY id");
        resultSet.close();
        statement.close();

        PreparedStatement preStmt = conn.prepareStatement("insert into test.tb_arr (id, name, vectors) select * from {tt 'raw_data'}");

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        out.write(new byte[]{3,0,0,0});
        String name = "foo";
        writeUnsignedLeb128(out, name.length());
        out.write(name.getBytes());

        byte[][] vectors = new byte[2][];
        vectors[0] = Base64.getDecoder().decode("LjMaNOg7ITRHLPs77zBVNIE69DtYM9Q2SjmCOUEyWDq0J0cu7jSbMUsuEDbQLmsz1DlEOGo2tzuLOoY6UzrzOToyPzWdO/E6YDoLMzUtZjfUMaE1Yzk0NMI4tTuwNls5UTWyMt8o/jioOHY7yjheODs5KjnRMEE37TZ6Lsg1PSivOkA6ySj8NQE44CwSK7Y7mzuVLqcaUzahOj844DhzMlc26i6SNz4zcTn4JEUxWzl1OTY2HjduNlQwTjntNpY7YTv5NxI6hTbUNvg2ADoXOMI7IjsnMfw4hThyNr0x+S1IOak3+ztHOog64zfHL3033jksNYA5rDKtJhc7oTIaOg==");
        vectors[1] = Base64.getDecoder().decode("QzuZL4k75To2O2w36jWtOOMweSy9OI814TpiOgk6WzlXOPE0HTp6O9g5kjUMOxE68zfhMHg4PjaIOLQ6gSnmO9k1OzqxMTg0gjWPNkcx9zloMDo42TEZOw00kTE9OK03/jkvO7U6CTLVMVU5rDjkOp80izrDNn8mlzQ+OZM6aCQoMCE5yTo5Mbo6CDkFOSo6/ThINMk1ujsrKeYzxTHXNGU5pjSAOvQ33DgpOOI3CjUYOUg2sDeYNaY7dDp9JTM2bzMlOu46Di/SO307lDeSLmEq/TcsOvU3VzjgNL0ypjqROjok+je4NT05ijs/Oi45SC9oOck3kzTGOJU2xzfEOA==");

        System.out.println("vectors[0] len: " + vectors[0].length + " vectors[1] len: " + vectors[1].length);

        writeUnsignedLeb128(out, vectors.length);
        for(byte[] vec : vectors) {
            writeUnsignedLeb128(out, vec.length);
            out.write(vec);
        }

        // 单个流继续写入多条
        out.write(new byte[]{6,0,0,0});
        String name1 = "bar";
        writeUnsignedLeb128(out, name1.length());
        out.write(name1.getBytes());

        writeUnsignedLeb128(out, vectors.length);
        for(byte[] vec : vectors) {
            writeUnsignedLeb128(out, vec.length);
            out.write(vec);
        }

        preStmt.setObject(1, ClickHouseExternalTable
                .builder().name("raw_data")
                .columns("id UInt32,name String,vectors Array(String)")
                .format(ClickHouseFormat.RowBinary)
                .content(new ByteArrayInputStream(out.toByteArray()))
                .build());

        // preStmt.executeUpdate();

        preStmt.addBatch();

        // 通过多个 out 多次写入
//        ByteArrayOutputStream out1 = new ByteArrayOutputStream();

//        out1.write(new byte[]{6,0,0,0});
//        String name1 = "bar";
//        writeUnsignedLeb128(out1, name1.length());
//        out1.write(name1.getBytes());
//
//        writeUnsignedLeb128(out1, vectors.length);
//        for(byte[] vec : vectors) {
//            writeUnsignedLeb128(out1, vec.length);
//            out1.write(vec);
//        }
//
//        preStmt.setObject(1, ClickHouseExternalTable
//                .builder().name("raw_data")
//                .columns("id UInt32,name String,vectors Array(String)")
//                .format(ClickHouseFormat.RowBinary)
//                .content(new ByteArrayInputStream(out1.toByteArray()))
//                .build());
//
//        preStmt.addBatch();

        preStmt.executeBatch();

        preStmt.close();

        conn.close();
    }
}
