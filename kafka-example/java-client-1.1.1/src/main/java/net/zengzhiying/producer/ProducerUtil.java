package net.zengzhiying.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka producer 工具类 使用javaapi Producer构建生产者
 * @author zengzhiying
 *
 */
public class ProducerUtil {
    
    private static Producer<Integer,String> producer;
    private static Properties props = new Properties();
    private static String topic = "message";
    private static String brokerList = "192.168.139.129:9092";

    static {
        
        //定义连接的broker list
        props.put("metadata.broker.list", brokerList);
        //定义对象传输之前序列化
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        producer = new Producer<Integer, String>(new ProducerConfig(props));
        
    }
    
    /**
     * 向kafka推送单条数据
     * @param message
     */
    public static void sendMessage(String message) {
        KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, message);
        //推送消息到broker
        producer.send(data);
    }
    
    /**
     * 向kafka批量推送list 消息
     * @param messages
     */
    public static void sendBatchMessage(List<String> messages) {
        List<KeyedMessage<Integer, String>> keyedMessages = new ArrayList<KeyedMessage<Integer, String>>();
        
        //先分组写入List，后续统一发送
        for(String msg: messages) {
            //构建消息对象
            KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, msg);
            keyedMessages.add(data);
        }
        //推送消息
        producer.send(keyedMessages);
    }

    public static void close() {
        producer.close();
    }
    
}
