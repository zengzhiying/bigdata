import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 读取activemq topic消息
 * @author zengzhiying
 */
public class FetchActiveMQTopic implements MessageListener {
	
	private static String USER = ConfigLoad.getConfig("user");
	private static String PASSWORD = ConfigLoad.getConfig("password");
	private static String URL = ConfigLoad.getConfig("mq_hosts");
	private static String SUBJECT = ConfigLoad.getConfig("mq_topic");
	private static Topic topic = null;
	private Connection conn = null;
	private Session session = null;
	private MessageConsumer consumer = null;
	private static String messageText = null;
	
    private void initialize() throws JMSException, Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USER, PASSWORD, URL);
        conn = connectionFactory.createConnection();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(SUBJECT);
        consumer = session.createConsumer(topic);
    }
	
	public void receiveMessage() throws JMSException, Exception {
        initialize();
        conn.start();
        while (true) {
            Message message = consumer.receive(1000 * 100);
            TextMessage text = (TextMessage) message;
            System.out.println("message: " + text.getText());
        }
    }

	@Override
	public void onMessage(Message msg) {
	    try {
            if (msg instanceof TextMessage) {
                TextMessage message = (TextMessage) msg;
                System.out.println("------Received TextMessage------");
                messageText = message.getText();
                System.out.println(messageText);
            } else {
                messageText = msg.toString();
                System.out.println(messageText);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                this.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
	}
	
	public void close() throws JMSException {
        System.out.println("Consumer:->Closing connection");
        if (consumer != null)
            consumer.close();
        if (session != null)
            session.close();
        if (conn != null)
            conn.close();
    }
	
	public static void main(String[] args) throws JMSException, Exception {
        System.out.println("初始化配置项:");
        System.out.println("用户名和密码:" + USER + " / " + PASSWORD);
        System.out.println("mq连接url:" + URL);
        System.out.println("my topic:" + SUBJECT);
        
        Thread.sleep(3000);
        
	    FetchActiveMQTopic fetchActiveMQ = new FetchActiveMQTopic();
        fetchActiveMQ.receiveMessage();
        fetchActiveMQ.close();
    }

}
