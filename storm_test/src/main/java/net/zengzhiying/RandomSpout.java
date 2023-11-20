package net.zengzhiying;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1215360697314209159L;
    private SpoutOutputCollector collector;
    private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};
    
    public void nextTuple() {
        String word = words[new Random().nextInt(words.length)];
        collector.emit(new Values(word));
    }

    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("randomstring"));
    }

}