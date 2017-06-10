package net.zengzhiying.bolts;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.zengzhiying.beans.TestBean;

public class DataParseBolt extends BaseRichBolt {

	private static final long serialVersionUID = 845009764287709672L;
	
	//进行相关的全局初始化
	private OutputCollector collector;

	/**
	 * 处理DataLogicBolt过来的数据 组装对象 发送给下一个bolt处理
	 */
	public void execute(Tuple input) {

		System.out.println("DataParseBolt.execute() start...");
		long start = System.currentTimeMillis();

		try {
			//获取kafka传过来的json字符串
			String data = input.getValue(0).toString();
			System.out.println("data string:" + data);
			//解析为对象
			TestBean tb = new Gson().fromJson(data, TestBean.class);
			System.out.println(tb.toString());
			//相关处理组装 清洗
			if(tb.getId() == null || tb.getId().equals("")) {
				tb.setId(UUID.randomUUID().toString());
			}
			Values tuple = new Values(tb);
			// 发送给下一个bolt处理
			this.collector.emit(input, tuple);
			//返回成功标识
			this.collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("DataParseBolt exception....");
			this.collector.fail(input);
		}

		long end = System.currentTimeMillis();
		System.out.println("DataParseBolt 执行完毕, 耗时:" + (end - start)/1000 + "s");

	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		//相关的初始化操作

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
