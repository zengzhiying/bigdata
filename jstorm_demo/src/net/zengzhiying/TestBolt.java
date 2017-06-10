package net.zengzhiying;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class TestBolt extends BaseRichBolt {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestBolt.class);
    OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        String message = input.getString(0);
        LOGGER.info(String.format("今天要学习： %s", message));
        System.out.println("xxx:" + message);
        // 发送成功返回ack
        this.collector.ack(input);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub
        
    }

}
