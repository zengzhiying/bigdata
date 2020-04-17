package example;

import com.alibaba.fastjson.annotation.JSONField;

public class DocumentBean {
    @JSONField(name = "name")
    private String name;
    @JSONField(name = "age")
    private int age;
    @JSONField(name = "id_number")
    private String idNumber;

    @Override
    public String toString() {
        return "DocumentBean{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", idNumber='" + idNumber + '\'' +
                ", key='" + key + '\'' +
                '}';
    }

    @JSONField(name = "_key")
    private String key = null;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getIdNumber() {
        return idNumber;
    }

    public void setIdNumber(String idNumber) {
        this.idNumber = idNumber;
    }

}
