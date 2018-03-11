package net.zengzhiying;

import java.util.List;

public class TestBean {
    private int id;
    private String name;
    private List<String> names;
    public List<String> getNames() {
        return names;
    }
    public void setNames(List<String> names) {
        this.names = names;
    }
    public TestBean(int id, String name, List<String> names) {
        this.id = id;
        this.name = name;
        this.names = names;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    
}
