package net.zengzhiying.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka clients new producer client示例 推荐使用
 */
public class KafkaProducerExample {
	private static Producer<String, String> producer;
	private Properties props=new Properties();
	
	public KafkaProducerExample(){
		//定义连接的broker list
		props.put("bootstrap.servers", "192.168.139.129:9092");
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
	
	public static void main(String[] args) {
		new KafkaProducerExample();
		//定义topic
        String topic="message";
        //推送消息到broker
        producer.send(new ProducerRecord<String, String>(topic, "Hello"));
        //方式2 打印偏移量
        producer.send(new ProducerRecord<String, String>(topic, ",World"), new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if(e != null) {
					e.printStackTrace();
				} else {
					//System.out.println(metadata.toString());
					//System.out.println(metadata.offset());
				}
			}
		});
        System.out.println("即将批量发送消息...");
        try {
			Thread.sleep(5000);
		} catch (InterruptedException e2) {
			e2.printStackTrace();
		}
        for(int i = 0;i < 1000;i++) {
        	producer.send(new ProducerRecord<String, String>(topic, "{'id':12,'cbu':'shc'}"));
        	try {
				Thread.sleep(10);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
        }
        producer.flush();
        producer.close();
	}
}
