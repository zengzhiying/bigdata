package net.zengzhiying;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * rocketmq 消费者示例
 * @author zengzhiying
 *
 */
public class ConsumerDemo {
    private static int number = 0;
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_group1");
        consumer.setNamesrvAddr("192.168.182.129:9876");
        // 设置从哪里开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅主题消费
        consumer.subscribe("testTopic", "*");
        // 注册到broker消费
        
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("thread: " + Thread.currentThread().getName());
                System.out.println("message: " + msgs);
                for(MessageExt msg: msgs) {
                    System.out.println(new String(msg.getBody()));
                    number++;
                }
                if(number % 10 == 0) {
                    System.out.println("条数: " + number);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.println("started consumer!");
    }
}
