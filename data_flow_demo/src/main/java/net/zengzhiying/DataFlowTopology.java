package net.zengzhiying;

import java.util.Arrays;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import net.zengzhiying.bolts.DataParseBolt;
import net.zengzhiying.bolts.MessageScheme;
import net.zengzhiying.bolts.SolrBolt;
import net.zengzhiying.tools.ConfigTool;

public class DataFlowTopology {
	
	//定义Topology所用到的全局配置 可以从配置文件中加载
	private static String kafkaZookeeper = ConfigTool.getConfig("kafkaZookeeper");
	private static String kafkaZkDir = ConfigTool.getConfig("kafkaZkDir");
	private static String topic = ConfigTool.getConfig("topic");
	private static String kafkaZkName = ConfigTool.getConfig("kafkaZkName");
	private static int maxSpoutPending = Integer.valueOf(ConfigTool.getConfig("passinfo_topology_max_spout_pending"));
	
	//获取本地模式配置
	private static String localZkServers = ConfigTool.getConfig("localZkServers");
	
	//设置storm相关线程数
	private static int topology_workers_num = Integer.valueOf(ConfigTool.getConfig("topology_workers_num"));
	private static int topology_ackers_num = Integer.valueOf(ConfigTool.getConfig("topology_ackers_num"));
	
	//设置各个bolt并行度参数
	private static int kafkaSpoutParallelism = Integer.valueOf(ConfigTool.getConfig("kafka_spout_parallelism"));
	private static int dataParseBoltParallelism = Integer.valueOf(ConfigTool.getConfig("data_parse_bolt_parallelism"));
	private static int solrBoltParallelism = Integer.valueOf(ConfigTool.getConfig("solr_bolt_parallelism"));
	
	
	@Option(name = "--remote", usage = "is remote or local:是否提交到远程模式")
	private boolean remote;
	@Option(name = "--from-beginning", usage = "读取kafka包含原始数据")
	private boolean fromStart;
	@Option(name = "--from-end", usage = "从最新 kafka数据读取")
	private boolean fromEnd;
	@Option(name = "--tn", usage = "提交toplogy的名字")
	private String topologyName = "defaultToplogy";

	public static void main(String[] args) {
		//做一些初始化处理
		// ...
		
		try {
			new DataFlowTopology().doMain(args);
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}

	public void doMain(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		// 一些初始化处理
		//..
		
		
		//命令行解析
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			e.printStackTrace();
		}
		
		// topology配置 start
		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper + kafkaZkDir);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, kafkaZkDir + "/stormconsumers", kafkaZkName);
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		//配置发生outofrange错误时 从初始位置读取数据
		spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
		
		Config conf = new Config();
		conf.setDebug(false);	// 生产一般改为false
		spoutConfig.socketTimeoutMs = 60 * 1000;
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 300);
		
		// storm从原始数据消费
		if (fromStart) {
			spoutConfig.ignoreZkOffsets = true;
			spoutConfig.startOffsetTime = -2;
		}

		// 从kafka最新的记录读取
		if (fromEnd) {
			spoutConfig.ignoreZkOffsets = true;
			spoutConfig.startOffsetTime = -1;
			System.out.println("集群从kafka最新的数据读取 ...");
		}
		
		//topology配置 end

		// 构建topology start
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig),kafkaSpoutParallelism);	// 从Kafka中读取数据
		
		builder.setBolt("DataParse", new DataParseBolt(), dataParseBoltParallelism).shuffleGrouping("kafkaSpout");	//第二个bolt 如数据整理
		
		builder.setBolt("SolrWrite", new SolrBolt(), solrBoltParallelism).shuffleGrouping("DataParse");	// 最终结果存储或其他处理，比如放入solr
		
		// 执行本地模式还是集群模式 本地模式是在一个JVM进程中多线程模拟
		if (remote) {
			System.out.println("提交到集群并运行 .....");
			conf.setNumWorkers(topology_workers_num);
			conf.setNumAckers(topology_ackers_num);
			try {
				StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
			} catch (AuthorizationException e) {
				//提交topology异常
				e.printStackTrace();
			}
		} else {
			System.out.println("本地模式运行.....");
			// 本地模式需要添加如下配置才能保存Offset
			spoutConfig.zkServers = Arrays.asList(localZkServers.split(","));
			spoutConfig.zkPort = 2181;
			// 提交topology到本地集群
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
			// 一段时间后终止集群运行
			Utils.sleep(100000);
			cluster.killTopology(topologyName);
			cluster.shutdown();
		}
	}
}
