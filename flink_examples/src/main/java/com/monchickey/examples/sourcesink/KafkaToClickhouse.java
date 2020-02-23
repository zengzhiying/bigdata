package com.monchickey.examples.sourcesink;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class KafkaToClickhouse {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "flink";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
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

        // kafka配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.128:9092");
        props.put("group.id", "image-label-20191216");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStream<ImageLabel> stream = env.addSource(new FlinkKafkaConsumer011<ImageLabel>(
                "imageLabel",
                new ImageLabelSchema(),
                props
        )).setParallelism(2);
        // label 处理
        SingleOutputStreamOperator<ImageLabel> processStream = stream.flatMap(new FlatMapFunction<ImageLabel, ImageLabel>() {
            @Override
            public void flatMap(ImageLabel imageLabel, Collector<ImageLabel> collector) throws Exception {
                if (null == imageLabel) {
                    System.out.println("image label 为空!");
                    return;
                }
                if (imageLabel.labelId < 0) {
                    System.out.println("label id错误! " + imageLabel.toString());
                    return;
                }
                if (imageLabel.labelId == 0) {
                    imageLabel.labelId = 99;
                    imageLabel.labelName = "未知.";
                }
                collector.collect(imageLabel);
            }
        }).setParallelism(2);

        // 入库.
        processStream.timeWindowAll(Time.seconds(10)).apply(new AllWindowFunction<ImageLabel, List<ImageLabel>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<ImageLabel> iterable, Collector<List<ImageLabel>> collector) throws Exception {
                ArrayList<ImageLabel> imageLabels = Lists.newArrayList(iterable);
                if(!imageLabels.isEmpty()) {
                    System.out.println("10s内批量条数: " + imageLabels.size());
                    collector.collect(imageLabels);
                }
            }
            // sink按照配置文件默认并行度是2, 会获取两份链接, 数据经过上面的分发, 所以不会重复
        }).addSink(new ClickhouseSink()).setParallelism(1);

        // 统计总条数
        Table table = tableEnv.fromDataStream(processStream, "labelId, labelName, UserActionTime.proctime");
        Table result = table
                .window(Tumble.over("1.minutes").on("UserActionTime").as("tWindow"))
                .groupBy("tWindow").select("count(labelId) as count, tWindow.start as start, tWindow.end as end");
        DataStream<LabelCount> tableStream = tableEnv.toAppendStream(result, LabelCount.class);
        tableStream.print();
        tableStream.flatMap(new FlatMapFunction<LabelCount, Object>() {
            @Override
            public void flatMap(LabelCount labelCount, Collector<Object> collector) throws Exception {
                System.out.println("1分钟统计: " + labelCount.count);
            }
        });

        // 统计分组条数
        processStream
                .map(new MapFunction<ImageLabel, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(ImageLabel imageLabel) throws Exception {
                        return new Tuple2(imageLabel.labelId, 1);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(60))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                }).flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, Object>() {
                    @Override
                    public void flatMap(Tuple2<Integer, Integer> t, Collector<Object> collector) throws Exception {
                        System.out.println("label: " + t.f0 + " 一分钟统计: " + t.f1);
                    }
                });

        env.execute("Kafka to Clickhouse");
    }
}
