package net.zengzhiying.httpclient;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * solr http方式写入solr数据
 * @author zengzhiying
 *
 */

public class SolrWriterTest {
	
	private static final String url = "http://192.168.1.79:8983/solr/zzy_collection";
	
	public static void main(String[] args) {
		SolrClient solr = new HttpSolrClient(url);
		SolrInputDocument document = new SolrInputDocument();
		document.addField("id", "102");
		document.addField("username", "test_username");
		document.addField("email", "abchs@email.com");
		document.addField("age", "25");
		document.addField("content","内容");
		try {
			solr.add(document);
			solr.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		} finally {
		    try {
                solr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
		}
	}
}
