import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Storm Topology 测试
 * @author zengzhiying
 *
 */

public class TestTopology {
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		//设置数据源
		builder.setSpout("stringspout", new StringSpout());
		//设置数据处理的bolt
		builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("stringspout");
		
		//配置conf
		Config conf = new Config();
		//生产过程中设置false
		conf.setDebug(false);
		
		if(args != null && args.length > 0) {
			//提交到集群运行
			//设置work个数
			conf.setNumWorkers(3);
			//提交Topo到集群运行
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
                e.printStackTrace();
            }
		} else {
			//本地模式运行
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local_topology", conf, builder.createTopology());
			//设置本地前台执行时间
			Utils.sleep(30000);
			//结束拓扑
			cluster.killTopology("local_topology");
			cluster.shutdown();
		}
	}
	
}
