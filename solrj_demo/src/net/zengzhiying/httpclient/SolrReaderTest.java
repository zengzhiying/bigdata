package net.zengzhiying.httpclient;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * solr http方式读取solr数据
 * @author zengzhiying
 *
 */

public class SolrReaderTest {
	
	private static final String url = "http://192.168.1.79:8983/solr/zzy_collection";
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SolrClient solr = new HttpSolrClient(url);
		SolrQuery query = new SolrQuery();
		query.set("fl", "username,email,content");
		query.set("q", "content:xxxxxxxx");
		try {
			QueryResponse response = solr.query(query);
			SolrDocumentList list = response.getResults();
			if(list.size() == 0) {
				System.out.println("查询到的数据为空...");
			} else {
				for(SolrDocument document:list) {
					String username = (String) document.getFieldValue("username");
					String email = (String) document.getFieldValue("email");
					String content = (String) document.getFieldValue("content");
					System.out.println("用户名:" + username + "邮箱:" + email + "内容:" + content);
				}
			}
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
}
