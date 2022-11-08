package com.monchickey.examples.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IterateExample {
    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {
        // 获得输入参数
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 获取执行环境, 设置buffer timeout为1启用连续刷新输出缓冲区, 低延迟
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);
        // 设置命令行参数试web界面可以查看
        env.getConfig().setGlobalJobParameters(params);


        // 创建input stream, 整数对
        DataStream<Tuple2<Integer, Integer>> inputStream;
        if(params.has("input")) {
            // 通过文件创建
            System.out.println("读取文件: " + params.get("input"));
            inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
        } else {
            System.out.println("Executing Iterate example with default input data set.");
            System.out.println("Use --input to specify file input.");
            inputStream = env.addSource(new RandomFibonacciSource());
        }

        // 创建输入流迭代, 超时时间为5s
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream
                .map(new InputMap())
                .iterate(5000);

        // 采用步进功能获取下一个斐波那契数, 递增计数器
        // 同时使用输出选择器拆分不同的输出.
        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it
                .map(new Step())
                .split(new MySelector());
        // 选择iterate的setp输出
        it.closeWith(step.select("iterate"));

        // 生成最终输出, 获得迭代器最大的输入对 默认计算滑窗: 1s
        DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step
                .select("output")
                .map(new OutputMap());

        // 输出
        if(params.has("output")) {
            // 输出到外部文件
            numbers.writeAsText(params.get("output"));
        } else {
            // 打印在标准输出
            numbers.print();
        }

        // 执行任务
        env.execute("Streaming Iteration Example.");
    }

    /**
     * 用户自定义sourceStream
     * 生成BOUND个从1到BOUND / 2范围内的随机整数对
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        private Random rand = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws Exception {
            while(isRunning && counter < BOUND) {
                int first = rand.nextInt(BOUND / 2 - 1) + 1;
                int second = rand.nextInt(BOUND / 2 - 1) + 1;

                sourceContext.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 文件输入清洗map自定义
    private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Integer> map(String value) throws Exception {
            String record = value.substring(1, value.length() - 1);
            String[] splitted = record.split(",");
            return new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
        }
    }

    // 自定义输入流迭代
    // 将输入整数对处理用于计算斐波那契数的原始值
    public static class InputMap implements MapFunction<Tuple2<Integer, Integer>,
            Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }

    // 迭代函数, 不断计算斐波那契数列
    public static class Step implements
            MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
                    Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
        }
    }

    // 选择需要再次迭代哪个元组
    public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
            List<String> output = new ArrayList<>();
            if(value.f2 < BOUND && value.f3 < BOUND) {
                output.add("iterate");
            } else {
                output.add("output");
                System.out.println(value);
            }
            System.out.println(output);
            return output;
        }
    }

    // 输出map操作
    public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
            Tuple2<Tuple2<Integer, Integer>, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
            return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
        }
    }
}
