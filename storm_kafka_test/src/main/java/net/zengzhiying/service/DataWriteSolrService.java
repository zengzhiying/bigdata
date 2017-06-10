package net.zengzhiying.service;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import net.zengzhiying.beans.DataBean;

public class DataWriteSolrService implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private static CloudSolrClient solr = null;
	// 单例模式 静态实例化
	private static DataWriteSolrService instance = null;
	
	//开始时间
	private static long startTime = System.currentTimeMillis();
	private static List<DataBean> datalist = Collections.synchronizedList(new ArrayList<DataBean>());
	
	//定义批量进入solr数据的大小
	private static final int BATCH_SIZE = 20;
	
	// 重写构造方法为私有 外部无法直接实例化
	private DataWriteSolrService() {
		
	}
	
	//线程安全获取实例
	public static synchronized DataWriteSolrService getInstance() {
		if(instance == null) {
			instance = new DataWriteSolrService();
		}
		return instance;
	}
	
	/*
	 * 建立连接
	 */
	static {
		solr = new CloudSolrClient("compute1:2181,compute2:2181,compute3:2181,compute4:2181,compute5:2181,compute6:2181/solr");
		solr.setDefaultCollection("test_collection");
		solr.setZkClientTimeout(20000);
		solr.setZkConnectTimeout(10000);
		solr.connect();
		
		getInstance();
		
	}
	
	/**
	 * 添加单条数据
	 * @param db
	 * @return status
	 */
	public int add(DataBean db) {
		int status = 0;
		try {
			solr.addBean(db);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	
	/**
	 * 分块批量添加数据
	 * @param db
	 */
	public void addBantch(DataBean db) {
		datalist.add(db);
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
	public void addAll(List<DataBean> datalist) {
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
