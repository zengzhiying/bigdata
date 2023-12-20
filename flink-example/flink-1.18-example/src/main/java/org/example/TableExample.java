package org.example;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableExample {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                // 流式模式 如果处理批量数据（比如文件）会输出中间所有的过程
//                .inStreamingMode()
                // 批量模式 对于批量数据直接得到最后的结果
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 从 Stream 转 Table
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> eleStream = sEnv.fromElements("alice", "bob", "maria");
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(sEnv);

        Table sTable = stEnv.fromDataStream(eleStream).as("name");
        stEnv.createTemporaryView("people", sTable);
        Table res = stEnv.sqlQuery("select * from people");
        res.execute().print();
        // 转为流执行则必须要执行 execute
        stEnv.toDataStream(res).print();
        // 保留所有的变化过程
//        stEnv.toChangelogStream(res).print();

        sEnv.execute("stream table job");


        // 随机序列生成器，无限流
//        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
//                .schema(Schema.newBuilder()
//                        .column("f0", DataTypes.STRING())
//                        .build())
//                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
//                .build();

        // 根据数据源创建表
//        tEnv.createTable("SourceTableA", sourceDescriptor);
        // 创建临时表
//        tEnv.createTemporaryTable("SourceTableB", sourceDescriptor);
        // 执行 SQL 也可以创建表
//        tEnv.executeSql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)")

//        Table tb = tEnv.from("SourceTableA");
//        tb.select($("f0")).execute().print();

        // 创建 schema
        final Schema schema = Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.BIGINT())
                .build();

        tEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "D:\\data.csv")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build());

        Table table = tEnv.sqlQuery("select b, sum(a) as sum_a from CsvSinkTable where c = 1002 group by b ");
        table.printSchema();
        table.printExplain();
        table.execute().print();

        table.filter($("b").isEqual("xiaofei"))
                .select($("b"), $("sum_a").as("s"))
                .execute().print();


        Table table2 = tEnv.sqlQuery("select a + 1 as a,b,c from CsvSinkTable where a = 9");


//        tEnv.createTemporaryTable("CsvSinkTable1", TableDescriptor.forConnector("filesystem")
//                .schema(schema)
//                .option("path", "D:\\data1.csv")
//                .format(FormatDescriptor.forFormat("csv")
//                        .option("field-delimiter", ",")
//                        .build())
//                .build());
        // 写入会持久化新的目录和文件
//        TablePipeline pipeline = table2.insertInto("CsvSinkTable1");
//        pipeline.printExplain();
//        pipeline.execute();
//
//        tEnv.executeSql("select a,b,c from CsvSinkTable1").print();

        // 创建临时的视图
        tEnv.createTemporaryView("CsvSinkTable1", table2);
        tEnv.executeSql("select a,b,c from CsvSinkTable union all select a,b,c from CsvSinkTable1").print();

    }
}
