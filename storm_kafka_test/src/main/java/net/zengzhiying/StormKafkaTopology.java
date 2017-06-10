package net.zengzhiying;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import net.zengzhiying.bolts.MessageScheme;
import net.zengzhiying.bolts.SenqueceBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class StormKafkaTopology {
    
    private static String zkServers = "bigdata:2181";
    private static String kafkaZkNode = "/kafka";
    private static String topic = "test";
    
	public static void main(String[] args) {
	    
		BrokerHosts brokerHosts = new ZkHosts(zkServers + kafkaZkNode);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, kafkaZkNode, "kafkaspout");
		
		Config conf = new Config();
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaspout", new KafkaSpout(spoutConfig));
		builder.setBolt("testbolt", new SenqueceBolt()).shuffleGrouping("kafkaspout");
		
		if(args != null && args.length > 0) {
			//提交到集群运行
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
			//本地模式运行
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("defaultTopology", conf, builder.createTopology());
			Utils.sleep(1000);
			cluster.killTopology("defaultTopology");
			cluster.shutdown();
		}
		
	}
}
