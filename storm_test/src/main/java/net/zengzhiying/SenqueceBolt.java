package net.zengzhiying;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SenqueceBolt extends BaseBasicBolt {

    /**
     * 
     */
    private static final long serialVersionUID = -1653344514833895244L;

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = (String) tuple.getValue(0);
        String out = "Hello " + word + "!";
        System.out.println(out);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

}