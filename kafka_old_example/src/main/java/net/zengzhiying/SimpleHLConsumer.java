package net.zengzhiying;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 此示例适用于 kafka_2.9.2-0.8.2.2 版本
 * @author zengzhiying
 *
 */

public class SimpleHLConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    
    public SimpleHLConsumer(String zookeeper, String groupId, String topic) {
        Properties props = new Properties();
        //定义连接zookeeper信息
        props.put("zookeeper.connect", zookeeper);
        //定义Consumer所有的groupID
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }

    public void testConsumer() {
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        //定义订阅topic数量
        topicCount.put(topic, new Integer(1));
        //返回的是所有topic的Map
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        //取出我们要需要的topic中的消息流
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
            while (consumerIte.hasNext())
                System.out.println("Message from Topic :" + new String(consumerIte.next().message()));
        }
        if (consumer != null)
            consumer.shutdown();
    }
    
    public static void main(String[] args) {
        String topic = "mytopic";
        SimpleHLConsumer simpleHLConsumer = new SimpleHLConsumer("192.168.1.216:2181/kafka", "testgroup", topic);
        simpleHLConsumer.testConsumer();
    }
}
