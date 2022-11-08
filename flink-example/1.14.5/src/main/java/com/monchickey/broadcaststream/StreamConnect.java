package com.monchickey.broadcaststream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

// nc -l -p 8980 写入数据
// nc -l -p 8981 写入规则

public class StreamConnect {
    public static void main(String[] args) throws Exception {

        System.out.println("启动.");

        MapStateDescriptor<String, Map<String, Integer>> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, Integer.class));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 8980);

        DataStreamSource<String> rules = env.addSource(new SocketTextStreamFunction("127.0.0.1", 8981, "\n", 3));

        BroadcastStream<Map<String, Integer>> broadcastStream = rules.map(new MapFunction<String, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> map(String s) throws Exception {
                System.out.println("收到广播流: " + s);
                String[] a = s.split(",");
                HashMap m = new HashMap<String, Integer>();
                m.put(a[0], Integer.parseInt(a[1]));
                return m;
            }
        }).broadcast(ruleStateDescriptor);

        source.connect(broadcastStream).process(new BroadcastProcessFunction<String, Map<String, Integer>, String>() {
            @Override
            public void processElement(String s, BroadcastProcessFunction<String, Map<String, Integer>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, Map<String, Integer>> state = readOnlyContext.getBroadcastState(ruleStateDescriptor);
                if(null == state || !state.contains("RulesBroadcastState")) {
                    collector.collect(s);
                    return;
                }
                Map<String, Integer> rule = state.get("RulesBroadcastState");
                System.out.println("当前广播的全部内容:");
                for(String k : rule.keySet()) {
                    System.out.println("key: " + k + " value: " + rule.get(k));
                }
                if(rule.containsKey(s)) {
                    collector.collect("replace: " + rule.get(s));
                } else {
                    collector.collect(s);
                }
            }

            @Override
            public void processBroadcastElement(Map<String, Integer> value, BroadcastProcessFunction<String, Map<String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                // 默认刷新广播流
                // context.getBroadcastState(ruleStateDescriptor).put("RulesBroadcastState", value);
                System.out.println("广播流合并.");
                // 增量更新广播流
                BroadcastState<String, Map<String, Integer>> state = context.getBroadcastState(ruleStateDescriptor);
                Map<String, Integer> current = state.get("RulesBroadcastState");
                System.out.println("current: " + current + " update: " + value);
                if(null == current) {
                    current = value;
                } else {
                    current.putAll(value);
                }
                state.put("RulesBroadcastState", current);
            }
        }).print();


        env.execute("broadcast stream");


    }
}
