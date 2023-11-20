package net.zengzhiying.beans;

import org.apache.solr.client.solrj.beans.Field;

/**
 * 实体类数据
 * @author zengzhiying
 *
 */

public class DataBean {
	private String id;
	private String username;
	private int age;
	
	public DataBean() {
		
	}
	
	public DataBean(String id, String username, int age) {
		this.id = id;
		this.username = username;
		this.age = age;
	}
	
	public String getId() {
		return id;
	}
	
	@Field("id")
	public void setId(String id) {
		this.id = id;
	}
	public String getUsername() {
		return username;
	}
	
	@Field("username")
	public void setUsername(String username) {
		this.username = username;
	}
	public int getAge() {
		return age;
	}
	
	@Field("age")
	public void setAge(int age) {
		this.age = age;
	}
	
	
}
