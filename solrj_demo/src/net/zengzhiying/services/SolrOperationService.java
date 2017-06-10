package net.zengzhiying.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;


/**
 * Solr相关方法服务
 * @author zengzhiying
 *
 */

public class SolrOperationService {
	
	private CloudSolrClient solr = null;
	
	public SolrOperationService(String zkHosts, String collectionName) {
		this.solr = new CloudSolrClient(zkHosts);
		solr.setDefaultCollection(collectionName);
	}
	
	
	/**
	 * 获取通用的对象返回结果集
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public List<Object> getBeanResults(Long startTime, Long endTime) {
	    // 测试条件
		String selectQuery = "test_time:[" + startTime + " TO " + endTime + "]";
		System.out.println(selectQuery);
		SolrQuery query = new SolrQuery();
		query.set("q", selectQuery);
		query.set("start", 0);
		query.set("rows", "1000000");
		QueryResponse response;
		try {
			response = solr.query(query);
			SolrDocumentList documents = response.getResults();
			if(documents == null) {
			    return null;
			}
			int num = documents.size();
			System.out.println("查询获取到的总条数为：" + num + "条！");
			if(num == 0) {
			    return null;
			} else {
			    return response.getBeans(Object.class);
			}
		} catch (SolrServerException e) {
			System.out.println("solr查询异常...");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("io异常...");
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 获取原始的documents结果集
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public String getResults(Long startTime, Long endTime) {
        String selectQuery = "test_time:[" + startTime + " TO " + endTime + "]";
        System.out.println(selectQuery);
        SolrQuery query = new SolrQuery();
        query.set("q", selectQuery);
        query.set("start", 0);
        query.set("rows", "1000000");
        QueryResponse response;
        try {
            response = solr.query(query);
            SolrDocumentList documents = response.getResults();
            if(documents == null) {
                return null;
            }
            int num = documents.size();
            System.out.println("查询获取到的总条数为：" + num + "条！");
            if(num == 0) {
                return null;
            } else {
                return documents.toString();
            }
        } catch (SolrServerException e) {
            System.out.println("solr查询异常...");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("io异常...");
            e.printStackTrace();
        }
        return null;
    }
	
	/**
	 * 获取通用的对象查询结果
	 * @param selectQuery
	 * @param classType
	 * @return
	 */
	public List<?> getQueryResults(String selectQuery, Class<?> classType) {
        System.out.println(selectQuery);
        SolrQuery query = new SolrQuery();
        query.set("q", selectQuery);
        query.set("start", 0);
        query.set("rows", "1000000");
        QueryResponse response;
        try {
            response = solr.query(query);
            List<?> infos = response.getBeans(classType);
            if(infos == null) {
                return null;
            }
            int num = infos.size();
            System.out.println("查询获取到的总条数为：" + num + "条！");
            if(num == 0) {
                return null;
            } else {
                return infos;
            }
        } catch (SolrServerException e) {
            System.out.println("solr查询异常...");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("io异常...");
            e.printStackTrace();
        }
        return null;
    }
	
	/**
	 * 批量添加通用的对象list
	 * @param infos
	 * @param groupNumber
	 * @throws InterruptedException
	 */
	public void addBatch(List<?> infos, int groupNumber) throws InterruptedException {
        List<Object> submitInfos = new ArrayList<Object>();
        int num = 0;
        for(Object info:infos) {
            submitInfos.add(info);
            if(submitInfos.size() == groupNumber) {
                try {
                    solr.addBeans(submitInfos);
                    solr.commit();
                } catch (SolrServerException | IOException e) {
                    System.out.println("向solr提交数据异常...");
                    e.printStackTrace();
                    Thread.sleep(3000);
                }
                submitInfos.clear();
                System.out.println("批量提交成功! " + groupNumber + "条.");
                num += groupNumber;
            }
        }
        // 最后不管剩余多少均提交
        if(submitInfos.size() > 0) {
            try {
                solr.addBeans(submitInfos);
                solr.commit();
                System.out.println("批量提交成功! " + submitInfos.size() + "条..");
            } catch (SolrServerException | IOException e) {
                System.out.println("向solr提交数据异常...");
                e.printStackTrace();
                Thread.sleep(3000);
            }
        }
        num += submitInfos.size();
        System.out.println("数据提交完毕...共:" + num + "条...");
    }
	
	/**
	 * 关闭solr连接
	 */
	public void close() {
		try {
			solr.close();
			System.out.println("close connection ok...");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("关闭solr连接异常...");
			e.printStackTrace();
		}
	}
	
}
