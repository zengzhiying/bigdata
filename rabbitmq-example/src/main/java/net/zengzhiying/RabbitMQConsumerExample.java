package net.zengzhiying;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * rabbitmq consumer example
 * @author ZengZhiying
 *
 */
public class RabbitMQConsumerExample {
    private static String USERNAME = "rabbit";
    private static String PASSWORD = "rabbit";
    private static String MQ_HOST = "192.168.0.23";
    private static int MQ_PORT = 5672;
    private static String VIRTUAL_HOST = "vhost1";
    private static String QUEUE_NAME = "rabbitExample";
    public static void main( String[] args ) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(MQ_HOST);
        factory.setPort(MQ_PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
        Connection connection = null;
        
        try {
            connection = factory.newConnection();
            Channel channel = connection.createChannel();
            
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println("start.");
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println("Received message: " + message);
                }
            };
            channel.basicConsume(QUEUE_NAME, true, consumer);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        } finally {
            if(connection != null && connection.isOpen()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
    }
}
