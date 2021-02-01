package net.zengzhiying;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 此示例适用于 kafka_2.9.2-0.8.2.2 版本
 * @author zengzhiying
 *
 */

public class SimpleProducer {
    private static Producer<Integer,String> producer;
    private final Properties props=new Properties();
    public SimpleProducer(){
        //定义连接的broker list
        props.put("metadata.broker.list", "192.168.1.216:9092");
        //定义序列化类 Java中对象传输之前要序列化
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("advertised.host.name", "192.168.1.216");
        producer = new Producer<Integer, String>(new ProducerConfig(props));
    }
    public static void main(String[] args) {
        new SimpleProducer();
        //定义topic
        String topic="mytopic";
        //开始时间统计
        long startTime = System.currentTimeMillis();
        //定义要发送给topic的消息
        String messageStr = "This is a message";
        List<KeyedMessage<Integer, String>> datalist = new ArrayList<KeyedMessage<Integer, String>>();
        //先分组写入List，后续统一发送
        for(int i = 0;i <= 1000;i++) {
        	//构建消息对象
            KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, messageStr);
            datalist.add(data);
        }
        //结束时间统计
        long endTime = System.currentTimeMillis();
        KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, "用时" + (endTime-startTime)/1000.0);
        datalist.add(data);
        //推送消息到broker
        producer.send(datalist);
        producer.close();
    }
}