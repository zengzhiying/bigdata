package net.zengzhiying.example2;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka Producer 发送消息工具类
 * @author zengzhiying
 *
 */

public class KafkaProducerUtils {
	private Producer<String, String> producer;
    private Properties props=new Properties();
    
    public KafkaProducerUtils(String cloudHosts){
        //定义连接的broker list
        props.put("bootstrap.servers", cloudHosts);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //保存配置
        producer = new KafkaProducer<String, String>(props);
    }
    
    /**
     * 向kafka推送消息
     * @param topicName
     * @param msg
     */
    public void sendMessage(String topicName, String msg) {
        //定义topic
        String topic = topicName;
        //推送消息到broker
        producer.send(new ProducerRecord<String, String>(topic, msg));
        producer.flush();
    }
    
    /**
     * 关闭kafka连接
     */
    public void closeKafkaProducer() {
        producer.close();
    }
}
