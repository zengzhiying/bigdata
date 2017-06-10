package net.zengzhiying.services;

/**
 * 采用单例模式获得solr连接
 * @author zengzhiying
 *
 */

public class SolrConnectionService {
	
	private static SolrOperationService solrConnection = null;
	
	private static final String zkHosts = "hostname1:2181,hostname2:2181,hostname3:2181";
	private static final String collection = "collection_name";
	
	static {
		solrConnection = new SolrOperationService(zkHosts, collection);
	}
	
	/**
	 * 获取solr连接
	 * @return
	 */
	public static SolrOperationService getSolrConnection() {
		if(solrConnection != null) {
			return solrConnection;
		}
		solrConnection = new SolrOperationService(zkHosts, collection);
		return solrConnection;
	}
	
	/**
	 * 重新获取solr连接
	 * @return
	 */
	public static SolrOperationService reloadSolrConnection() {
		if(solrConnection != null) {
			solrConnection.close();
		}
		solrConnection = new SolrOperationService(zkHosts, collection);
		return solrConnection;
	}
	
}
