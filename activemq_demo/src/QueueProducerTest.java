
public class QueueProducerTest {
    private static String userName = ConfigLoad.getConfig("user");
    private static String password = ConfigLoad.getConfig("password");
    private static String brokers = ConfigLoad.getConfig("mq_hosts");
    private static String queueName = ConfigLoad.getConfig("queue");
    public static void main(String[] args) {
        ActiveMQQueueProcucer producer = new ActiveMQQueueProcucer(userName, password, brokers, queueName);
        if(producer.initConnection()) {
            System.out.println("mq初始化成功.");
//            producer.sendMessage("test message");
//            producer.sendMessage("test message2");
//            producer.sendMessage("test message3");
            producer.sendMessage(args[0]);
            System.out.println("close.");
            producer.close();
        } else {
            System.out.println("mq初始化失败！");
        } 
    }
}
