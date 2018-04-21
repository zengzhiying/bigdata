package net.zengzhiying;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * rocketmq 生产者示例
 * @author Administrator
 *
 */
public class BatchProducerDemo 
{
    public static void main( String[] args ) throws MQClientException, UnsupportedEncodingException {
        DefaultMQProducer producer = new DefaultMQProducer("test_group1");
        producer.setNamesrvAddr("192.168.182.129:9876");
        producer.start();
        List<Message> messages = new ArrayList<>();
        for(int i = 0; i < 521; i++) {
            Message message = new Message("testTopic",
//                    "tag", 标签
//                    "key", key
                    ("" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            messages.add(message);
            try {
                if(messages.size() >= 100) {
                    SendResult sendResult = producer.send(messages);
                    System.out.println(sendResult);
                    messages.clear();
                }
            } catch (RemotingException | MQBrokerException | InterruptedException e) {
                System.out.println("发送消息失败！");
                e.printStackTrace();
            }
        }
        if(messages.size() > 0) {
            try {
                producer.send(messages);
                System.out.println("最后一批发送完毕.");
            } catch (RemotingException | MQBrokerException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
