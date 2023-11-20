package net.zengzhiying.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.zengzhiying.beans.DataBean;
import net.zengzhiying.service.DataWriteSolrService;

public class SenqueceBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String text = (String) tuple.getValue(0);
		System.out.println(text);
		// 调用数据写入的方法写入到文件、solr、Hbase等
		//字符串拆分为数组
		//String[] textArr = text.split(";");
		//构造bean
//		DataBean db = new DataBean();
//		db.setId(textArr[0].split(":")[1]);
//		db.setUsername(textArr[1].split(":")[1]);
//		db.setAge(Integer.valueOf(textArr[2].split(":")[1]));
//		//入solr
//		DataWriteSolrService swss = DataWriteSolrService.getInstance();
//		swss.addBantch(db);
		
		//发送至下一个bolt
		collector.emit(new Values(text));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("message"));
	}
	
	public static void main(String[] args) {
		String text = "id:1;username:obj;age:23";
		String[] textArr = text.split(";");
		//构造bean
		DataBean db = new DataBean();
		db.setId(textArr[0].split(":")[1]);
		db.setUsername(textArr[1].split(":")[1]);
		db.setAge(Integer.valueOf(textArr[2].split(":")[1]));
		//入solr
		DataWriteSolrService swss = DataWriteSolrService.getInstance();
		
		swss.addBantch(db);
		
	}

}
