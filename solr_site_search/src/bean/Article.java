package bean;

public class Article {
	private String id;
	private String title;
	private String abs;
	private String content;
	
	public Article() {
		
	}
	
	public Article(String id, String title, String abs, String content) {
		this.id = id;
		this.title = title;
		this.abs = abs;
		this.content = content;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getAbs() {
		return abs;
	}
	public void setAbs(String abs) {
		this.abs = abs;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	
	
}
