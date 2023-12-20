package com.monchickey.examples.sourcesink;

import com.alibaba.fastjson.JSON;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class KafkaToKafka {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // kafka配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.128:9092");
        props.put("group.id", "student-20191215");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStream<Student> stream = env.addSource(new FlinkKafkaConsumer011<>(
                "student",
                new SimpleStringSchema(),
                props
        )).setParallelism(2).map(value -> JSON.parseObject(value, Student.class));

        stream.timeWindowAll(Time.seconds(10)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            // 5s批量处理
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Student> iterable, Collector<List<Student>> collector) throws Exception {
                List<Student> students = new ArrayList<>();
                Iterator<Student> stuIter = iterable.iterator();
                while(stuIter.hasNext()) {
                    students.add(stuIter.next());
                }
                System.out.println("10s内数据条数: " + students.size());
                if(!students.isEmpty()) {
                    collector.collect(students);
                }
            }
        });

        DataStream<Student> cleanStream = stream.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student student, Collector<Student> collector) throws Exception {
                if(student.getAge() < 18) {
                    System.out.println("no." + student.toString());
                } else {
                    System.out.println("yes." + student.toString());
                    student.setId(student.getId() + 1);
                    collector.collect(student);
                }
            }
        });


        stream
                .map(new MapFunction<Student, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Student student) throws Exception {
                        return new Tuple2<Integer, Integer>(student.getAge(), 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                        return 0;
                    }
                }).timeWindow(Time.seconds(30))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        // 统计条数, 至少第二条数据到来才会执行
                        return new Tuple2<>(v1.f0, v1.f1 + 1);
                    }
                }).
                process(new ProcessFunction<Tuple2<Integer, Integer>, Integer>() {
                    @Override
                    public void processElement(Tuple2<Integer, Integer> value, Context context, Collector<Integer> collector) throws Exception {
                        System.out.println("平均30s 年龄: " + value.f0 + " 数据条数: " + value.f1);
                    }
                });

        cleanStream.map(student -> {
                    return JSON.toJSONString(student);
                }).addSink(new FlinkKafkaProducer011<String>(
                        "192.168.111.128:9092",
                        "cleanStudent",
                        new SimpleStringSchema()
                )).name("flink producer kafka.").setParallelism(2);

        env.execute("flink to kafka");
    }
}
