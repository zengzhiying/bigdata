package net.zengzhiying.zkprocess;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * zookeeper客户端常用操作
 * @author zengzhiying
 *
 */

public class ClientHandle {
    public static void main(String[] args) {
        try {
            ZooKeeper zk = new ZooKeeper("192.168.28.128:2181", 3000, 
                    new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            System.out.println("触发 ->" + event.getType() + "事件");
                        }
                
            });
            
            // 创建节点
            zk.create("/ceshi2", "to_data1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            
            // 读取节点数据
            System.out.println(new String(zk.getData("/ceshi2", false, null)));
            
            // 取出子目录节点列表
            System.out.println(zk.getChildren("/solr", true));
            
            // 修改节点数据
            zk.setData("/ceshi2", "to_data3".getBytes(), -1);
            byte[] data1 = zk.getData("/ceshi2", true, null);
            String datastr = new String(data1, "utf-8");
            System.out.println(datastr);
            
            
            // 删除节点 后面的整数为删除的版本号 -1代表删除所有版本 必须删除空的节点才可以
            //zk.delete("/ceshi2/abcs", -1);
            
            System.out.println("关闭zookeeper连接...");
            zk.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
