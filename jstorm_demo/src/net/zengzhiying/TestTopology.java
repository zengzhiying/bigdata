package net.zengzhiying;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TestTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testspout", new TestSpout(), 1);
        builder.setBolt("testbolt", new TestBolt(), 2).shuffleGrouping("testspout");
        
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);
        conf.setNumAckers(1);
        
        try {
            StormSubmitter.submitTopology("testtopology", conf, builder.createTopology());
            System.out.println("storm cluster will start...");
        } catch (AlreadyAliveException | InvalidTopologyException e) {
            System.out.println("submit topology error!");
            e.printStackTrace();
        }
        
    }
}
