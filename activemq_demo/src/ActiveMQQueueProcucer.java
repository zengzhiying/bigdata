import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * activemq操作工具类 queue队列生产
 * @author zengzhiying
 *
 */
public class ActiveMQQueueProcucer {
    
    // 配置选项  0不打印推送结果 1打印
    private int isDebug = Integer.valueOf(ConfigLoad.getConfig("is_debug"));
    
    // 定义工厂
    private ConnectionFactory factory = null;
    // 创建连接
    private Connection connection = null;
    // 创建会话
    private Session session = null;
    // 目的地
    private Destination destination = null;
    // 生产者
    private MessageProducer producer = null;
    
    // mq连接参数
    private String userName;
    private String password;
    private String brokers;
    private String queueName;
    
    /**
     * activeMQ服务类实例化构造方法
     * @param userName
     * @param password
     * @param brokers
     * @param queue
     */
    public ActiveMQQueueProcucer(String userName, String password, String brokers,
            String queue) {
        this.userName = userName;
        this.password = password;
        this.brokers = brokers;
        this.queueName = queue;
    }
    
    /**
     * 初始化mq连接和资源
     * @return
     */
    public boolean initConnection() {
        factory = new ActiveMQConnectionFactory(userName, password, brokers);
        try {
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(queueName);
            producer = session.createProducer(destination);
            // 设置生产者模式当mq异常的时候消息将会被保存
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            return true;
        } catch (JMSException e) {
            System.out.println("activeMQ初始化异常！");
            e.printStackTrace();
            // 关闭资源
            close();
            return false;
        }
    }
    
    /**
     * 向active mq发送消息
     * @param message
     */
    public void sendMessage(String message) {
        try {
            TextMessage textMessage = session.createTextMessage(message);
//            textMessage.setText("");
            if(isDebug == 1) {
                System.out.println("mq: " + message);
            }
            producer.send(textMessage);
//            session.commit();
        } catch (JMSException e) {
            System.out.println("向activemq发送消息失败！");
            e.printStackTrace();
        }
    }
    
    /**
     * 关闭mq连接 包括生产者,会话,连接
     */
    public void close() {
        try {
            if(producer != null) {
                producer.close();
            }
            if(session != null) {
                session.close();
            }
            if(connection != null) {
                connection.stop();
                connection.close();
            }
        } catch (JMSException e) {
            System.out.println("关闭连接异常！");
            e.printStackTrace();
        }
    }
}
