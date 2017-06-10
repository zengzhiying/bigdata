
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * storm Bolt 业务计算执行类
 *
 */

public class SenqueceBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//接收数据源发送过来的数据
		String word = (String) tuple.getValue(0);
		//数据的操作(处理，计算等)
		String out = "Hello " + word + " !";
		//数据的输出或存放
		System.out.println(out);
		
		//发送至下一个bolt
		collector.emit(new Values(out));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
