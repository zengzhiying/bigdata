package net.zengzhiying.example2;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka new consumer api 示例
 * @author zengzhiying
 *
 */

public class ConsumerExample {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "bigdata:9092");
	    props.put("group.id", "test");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    String topic = "test";
	    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	    // 设置要监听的topic列表
	    consumer.subscribe(Arrays.asList(topic, "test1"));
	    while (true) {
	    	ConsumerRecords<String, String> records = consumer.poll(100);
	        for (ConsumerRecord<String, String> record : records)
	        	//查看详细数据信息 或者做其他操作
	        	//System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	        	System.out.println("offset:" + record.offset() + "msg_value:" + record.value());
	    }
	}
	
}
