package com.monchickey.examples.sourcesink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaToClickhouseStream {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // kafka配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.128:9092");
        props.put("group.id", "image-body-20191223");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStream<ImageBody> stream = env.addSource(new FlinkKafkaConsumer011<ImageBody>(
                "imageBody",
                new ImageBodySchema(),
                props
        )).setParallelism(2);

        SingleOutputStreamOperator<List<ImageBody>> flatStream = stream.flatMap(new FlatMapFunction<ImageBody, List<ImageBody>>() {
            @Override
            public void flatMap(ImageBody imageBody, Collector<List<ImageBody>> collector) throws Exception {
                if (null == imageBody) {
                    System.out.println("image body is null.");
                    return;
                }
                if (imageBody.imageBody.length == 0) {
                    System.out.println("image body content is empty!" + imageBody.toString());
                    return;
                }
                List<ImageBody> imageBodys = new ArrayList<>(1);
                imageBodys.add(imageBody);
                collector.collect(imageBodys);
            }
        }).setParallelism(2);

        flatStream.addSink(new ClickhouseStreamSink()).setParallelism(2);

        env.execute("kafka to clickhouse stream.");
    }
}
