import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 数据源类
 *
 */

public class StringSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	private static String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};

	@Override
	public void nextTuple() {
	    //这里简单获取了随机数据，实际中可以从存储中批量取数据或者其他数据源获取
	    String word = words[new Random().nextInt(words.length)];
		try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		//分发数据至supervisor下游服务器处理
        collector.emit(new Values(word));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map arg0, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("randomstring"));
	}

}
