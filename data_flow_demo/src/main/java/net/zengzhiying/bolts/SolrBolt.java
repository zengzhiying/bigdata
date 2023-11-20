package net.zengzhiying.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import net.zengzhiying.beans.TestBean;
import net.zengzhiying.service.WriteSolrService;

/**
 * 写入数据到Solr bolt
 * @author zengzhiying
 *
 */
public class SolrBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8745626403669827481L;
	
	//相关变量的全局定义和初始化
	private OutputCollector collector;
	
	private static WriteSolrService wss = WriteSolrService.getInstance();
	
	public void execute(Tuple input) {
		try {
			//获取对象后续进行类型转换
			Object data = input.getValue(0);
			TestBean tb = (TestBean) data;
			
			//执行写入solr 操作
			if(tb != null) {
				wss.addBantch(tb);
			}
			
			// 这里不发送给下一个bolt处理  以下代码注释
//			Values tuple = new Values(passInfo);
//			this.collector.emit(input, tuple);
			//返回成功标识
			this.collector.ack(input);
			System.out.println("Solr Bolt....execute ok....");
		} catch (Exception e) {
			e.printStackTrace();
			this.collector.fail(input);
			System.out.println("Solr Bolt....execute 执行异常...");
		}

	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		//相关数据和配置初始化操作
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 不往下一个bolt发送则下一行不需要
		//declarer.declare(new Fields("message"));
	}
	
	public static void main(String[] args) {
	    //本地入solr测试方法
        TestBean tb = new TestBean();
        tb.setId("shshshshshs");
        tb.setCode(19282);
        wss.addBantch(tb);
    }
	
}
