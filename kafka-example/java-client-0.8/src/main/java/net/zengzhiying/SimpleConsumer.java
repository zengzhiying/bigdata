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
import kafka.message.MessageAndMetadata;

/**
 * 此示例适用于 kafka_2.9.2-0.8.2.2 版本
 * @author zengzhiying
 *
 */

public class SimpleConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    
    public SimpleConsumer(String zookeeper, String groupId, String topic) {
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
            ConsumerIterator<byte[], byte[]> consumerIter = stream.iterator();
            while (consumerIter.hasNext()) {
                MessageAndMetadata<byte[], byte[]> message = consumerIter.next();
                // 二进制消息
                byte[] msgValue = message.message();

                System.out.println(String.format("Message from topic [%s]: %s, partition: %d offset: %d",
                        message.topic(), new String(msgValue), message.partition(), message.offset()));
            }
        }
        if (consumer != null)
            consumer.shutdown();
    }
    
    public static void main(String[] args) {
        String topic = "mytopic";
        SimpleConsumer simpleHLConsumer = new SimpleConsumer("192.168.1.216:2181/kafka", "testgroup", topic);
        simpleHLConsumer.testConsumer();
    }
}
