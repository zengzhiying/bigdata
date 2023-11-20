package net.zengzhiying.service;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import net.zengzhiying.tools.ConfigTool;

public class WriteSolrService implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private static CloudSolrClient solr = null;
	// 单例模式 静态实例化
	private static WriteSolrService instance = null;
	
	private static String zkhosts = ConfigTool.getConfig("solr_zk_hosts");
	private static String zksolrroot = ConfigTool.getConfig("zk_solr_root");
	private static String collectName = ConfigTool.getConfig("collection_name");
	
	//开始时间
	private static long startTime = System.currentTimeMillis();
	private static List<Object> datalist = Collections.synchronizedList(new ArrayList<Object>());
	
	//定义批量进入solr数据的大小
	private static final int BATCH_SIZE = Integer.parseInt(ConfigTool.getConfig("solr_batch_size"));
	
	// 重写构造方法为私有 外部无法直接实例化
	private WriteSolrService() {
		
	}
	
	//线程安全获取实例
	public static synchronized WriteSolrService getInstance() {
		if(instance == null) {
			instance = new WriteSolrService();
		}
		return instance;
	}
	
	/*
	 * 建立连接
	 */
	static {
		solr = new CloudSolrClient(zkhosts+zksolrroot);
		solr.setDefaultCollection(collectName);
		solr.setZkClientTimeout(20000);
		solr.setZkConnectTimeout(10000);
		solr.connect();
		
		getInstance();
		
	}
	
	/**
	 * 添加单条数据
	 * @param vb
	 * @return status
	 */
	public int add(Object obj) {
		int status = 0;
		try {
			solr.addBean(obj);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	
	/**
	 * 分块批量添加数据
	 * @param vb
	 */
	public void addBantch(Object obj) {
		datalist.add(obj);
		//大于分组，立即提交
		if(datalist.size() >= BATCH_SIZE) {
			commitBeans();
		} else {
			//根据时间超过一定时长数据不够，全部提交
			long endTime = System.currentTimeMillis();
			if((endTime - startTime) > 3000) {
				commitBeans();
			}
		}
	}
	
	/**
	 * 提交所有数据并重置时间
	 */
	private void commitBeans() {
		
		synchronized(datalist) {
			try {
				solr.addBeans(datalist);
				solr.commit();
				datalist.clear();
				startTime = System.currentTimeMillis();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	
	/**
	 * 手动添加全部
	 * @param datalist
	 */
	public void addAll(List<Object> datalist) {
		try {
			solr.addBeans(datalist);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 手动提交与前面结合在适当时刻调用从而提高效率
	 */
	public void solrCommit() {
		try {
			solr.commit();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
