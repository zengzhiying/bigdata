package net.zengzhiying.consumer;
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
 * javaapi zookeeper方式消费kafka数据示例 老版本不推荐(1.x废弃)
 * @author zengzhiying
 *
 */

public class ZookeeperConsumerExample {
    private final ConsumerConnector consumer;
    private final String TOPIC = "message";
    private static String ZOOKEEPER_SESSION_TIMEOUT = "6000";
    private static String ZOOKEEPER_SYNC_TIME = "2000";
    private static String AUTO_COMMIT_INTERVAL = "5000";

    public ZookeeperConsumerExample(String zookeeper, String groupId) {
        Properties props = new Properties();
        //定义连接zookeeper信息
        props.put("zookeeper.connect", zookeeper);
        //定义Consumer所有的groupID
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", ZOOKEEPER_SESSION_TIMEOUT);
        props.put("zookeeper.sync.time.ms", ZOOKEEPER_SYNC_TIME);
        props.put("auto.commit.interval.ms", AUTO_COMMIT_INTERVAL);
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void readConsumer() {
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        //定义订阅topic数量
        topicCount.put(TOPIC, new Integer(1));
        //返回的是所有topic的Map
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        //取出需要的topic中的消息流
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(TOPIC);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
            while (consumerIte.hasNext())
                System.out.println("Message from Topic :" + new String(consumerIte.next().message()));
        }
        if (consumer != null)
            consumer.shutdown();
    }

    public static void main(String[] args) {
        ZookeeperConsumerExample cd = new ZookeeperConsumerExample("192.168.139.129:2181/kafka",
                "consumer-example-1101");
        cd.readConsumer();
    }

}