package net.zengzhiying;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * rocketmq 生产者示例
 * 并发消费条件下不能保证不重复 需要用户自己控制偏移
 * @author Administrator
 *
 */
public class ProducerDemo 
{
    public static void main( String[] args ) throws MQClientException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("test_group2");
        producer.setNamesrvAddr("192.168.182.129:9876");
        producer.start();
        for(int i = 0; i < 1000; i++) {
            Message message = new Message("testTopic1",
//                    "tag", 标签
//                    "key", key
                    ("" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            try {
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult); 
            } catch (RemotingException | MQBrokerException | InterruptedException e) {
                System.out.println("发送消息失败！");
                e.printStackTrace();
            }
        }
        
        producer.shutdown();
    }
}
