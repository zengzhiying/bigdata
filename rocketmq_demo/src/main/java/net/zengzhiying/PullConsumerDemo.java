package net.zengzhiying;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 客户端主动控制拉取rocketmq消息
 * @author Administrator
 *
 */
public class PullConsumerDemo {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("test_group1");
        consumer.setNamesrvAddr("192.168.182.129:9876");
        consumer.start();
        
        int number = 0;

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("testTopic1");
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue: " + mq + "%n");
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                        consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 64);
                    System.out.printf("%s%n", pullResult);
                    List<MessageExt> messages = pullResult.getMsgFoundList();
                    if(messages != null && !messages.isEmpty()) {
                        for(MessageExt msg: messages) {
                            System.out.println(new String(msg.getBody()));
                            number++;
                        }
                    } else {
                        System.out.println("empty!");
                    }
                    System.out.println("共处理条数. " + number);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        consumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}
