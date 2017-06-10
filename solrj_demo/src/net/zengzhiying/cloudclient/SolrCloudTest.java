package net.zengzhiying.cloudclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class SolrCloudTest {
	private static final String zkhost = "192.168.1.184,192.168.1.220,192.168.1.160,192.168.1.158,192.168.1.146,192.168.1.170/solr";
	private static final String collections = "my_collection";
	
	/**
	 * 批量写入
	 */
	public static void writeBatch() {
		CloudSolrClient solr  = new CloudSolrClient(zkhost);
		solr.setDefaultCollection(collections);
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		for(int i = 1;i <= 1000;i++) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", String.valueOf(i));
			doc.addField("name","test");
			doc.addField("title", "biaoti");
			docs.add(doc);
		}
		
		// 最终提交操作
		try {
			solr.add(docs);
			solr.commit();
			solr.close();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 写入单条数据
	 * @param id
	 * @param name
	 * @param title
	 */
	public static void writeSingle(String id, String name, String title) {
		CloudSolrClient solr = new CloudSolrClient(zkhost);
		solr.setDefaultCollection(collections);
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", id);
		doc.addField("name", name);
		doc.addField("title", title);
		
		try {
			solr.add(doc);
			solr.commit();
			solr.close();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 按照solr id删除单条数据
	 * @param id
	 */
	public static void deleteSingle(String id) {
		CloudSolrClient solr = new CloudSolrClient(zkhost);
		solr.setDefaultCollection(collections);
		try {
			solr.deleteById(id);
			solr.commit();
			solr.close();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 按照id批量删除多条数据
	 * @param ids
	 */
	public static void deleteBatch(List<String> ids) {
		CloudSolrClient solr = new CloudSolrClient(zkhost);
		solr.setDefaultCollection(collections);
		try {
			solr.deleteById(ids);
			solr.commit();
			solr.close();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//单条删除
		//DataDeleteOne("2");
		//批量删除
//		List<String> ids = new ArrayList<String>();
//		for(int i = 1;i <= 100;i++) {
//			ids.add(String.valueOf(i));
//		}
//		DataDeleteAll(ids);
		
		long startTime = System.currentTimeMillis();
		//单条添加
//		for(int i = 1;i <= 100;i++) {
//			DataWriteOne(String.valueOf(i), "zzy", "test");
//		}
		//批量添加
//		DataWriteAll();
		long endTime = System.currentTimeMillis();
		System.out.println("用时：" + (endTime-startTime)/1000.00 + "s");
	}
}
