package net.zengzhiying.beans;

import java.io.Serializable;

import org.apache.solr.client.solrj.beans.Field;

/**
 * 测试实体类
 * @author zengzhiying
 *
 */
public class TestBean implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;
	
	private String id;
	private int code;
	private String username;
	
	public String getId() {
		return id;
	}
	
	//设置入solr的字段
	@Field("id")
	public void setId(String id) {
		this.id = id;
	}
	public int getCode() {
		return code;
	}
	
	@Field("code")
	public void setCode(int code) {
		this.code = code;
	}
	public String getUsername() {
		return username;
	}
	
	@Field("username")
	public void setUsername(String username) {
		this.username = username;
	}

    @Override
    public String toString() {
        return "TestBean [id=" + id + ", code=" + code + ", username=" + username + "]";
    }

}
