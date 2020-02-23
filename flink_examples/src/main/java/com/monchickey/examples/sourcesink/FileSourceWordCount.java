package com.monchickey.examples.sourcesink;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FileSourceWordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.addSource(new FileSource())
                .setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStream = stream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        if (null == s || s.equals("")) {
                            System.out.println("字符串为空!");
                            return;
                        }
                        String[] words = s.split(" ");
                        System.out.println("单词个数: " + words.length);
                        for (String word : words) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                }).keyBy(0).timeWindow(Time.seconds(5)).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        System.out.println("reduce: " + t1 + " " + t2);
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                }).setParallelism(2);
        outputStream.print();
        env.execute("FileSource word count.");
    }
}
