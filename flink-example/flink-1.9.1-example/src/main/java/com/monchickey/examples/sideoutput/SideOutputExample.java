package com.monchickey.examples.sideoutput;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 带旁路输出的流处理
 */
public class SideOutputExample {
    // 需要创建outputTag来在操作中检索侧面输出流
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {};

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ProcessingTime: 以operator处理时间为基准
        // IngestionTime: 以数据进入flink streaming的时间为准
        // EventTime: 以数据自带的时间戳为准, 但应用程序要自己实现指定如何从record中抽取时间戳
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        if(params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            text = env.fromElements(WordCountData.WORDS);
        }

        //构建旁路输出流处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .keyBy(new KeySelector<String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Integer getKey(String s) throws Exception {
                        return 0;
                    }
                })
                .process(new Tokenizer());


        // 旁路流处理
        DataStream<String> rejectedWords = tokenized
                .getSideOutput(rejectedWordsTag)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "rejected: " + value;
                    }
                });
        // 主线流处理
        DataStream<Tuple2<String, Integer>> counts = tokenized
                .keyBy(0)
                // 自然时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 按照0分组, 然后按照1求和, 相当于reduce.
                .sum(1);
                // .reduce((a, b)-> new Tuple2<>(a.f0, a.f1 + b.f1));
        counts.print();
        rejectedWords.print();

        env.execute("Streaming WordCount SideOutput");
    }

    // 自定义分词处理器
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(String value, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for(String token : tokens) {
                if(token.length() > 5) {
                    // 侧面输出
                    context.output(rejectedWordsTag, token);
                } else if(token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
