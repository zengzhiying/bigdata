package model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import bean.Article;

public class QueryModel {
	
	public static final String url = "http://192.168.1.79:8983/solr/zhanneiSearch";
	
	@SuppressWarnings("resource")
    public static List<Article> query(String word) {
		
		List<Article> data = new ArrayList<Article>();
		SolrClient solr = new HttpSolrClient(url);
		SolrQuery q = new SolrQuery();
		q.set("fl", "id,title,abstract,content");
		q.set("q", "title:" + word);
		try {
			QueryResponse response = solr.query(q);
			SolrDocumentList document_list = response.getResults();
			
			if(document_list.size() == 0) {
				return null;
			} else {
				for(SolrDocument document:document_list) {
					//取数据
					String id = (String) document.getFieldValue("id");
					String title = (String) document.getFieldValue("title");
					String abs = (String) document.getFieldValue("abstract");
					String content = (String) document.getFieldValue("content");
					
					Article a = new Article(id, title, abs, content);
					
					data.add(a);
				}
			}
			
			return data;
			
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	
	@SuppressWarnings("resource")
    public static String[] queryOne(String id) {
		
		String[] data = new String[3];
		
		SolrClient solr = new HttpSolrClient(url);
		SolrQuery q = new SolrQuery();
		q.set("fl", "id,title,content");
		q.set("q", "id:" + id);
		QueryResponse response;
		try {
			response = solr.query(q);
			SolrDocumentList detail = response.getResults();
			if(detail.size() == 0) {
				return null;
			} else {
				for(SolrDocument sd:detail) {
					data[0] = (String) sd.getFieldValue("id");
					data[1] = (String) sd.getFieldValue("title");
					data[2] = (String) sd.getFieldValue("content");
				}
			}
			
			return data;
		} catch (SolrServerException | IOException e) {
			
			e.printStackTrace();
		}
		
		return null;
		
	}
	
	public static void main(String[] args) {
//		List<Article> data = new ArrayList<Article>();
//		
//		data = query("春天");
//		
//		System.out.println("条数：" + data.size());
//		
//		for(Article a:data) {
//			System.out.println(a.getContent());
//		}
		
		String[] data = queryOne("1");
		System.out.println(data[0]);
		System.out.println(data[1]);
		System.out.println(data[2]);
		
	}
	
}
