package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

public class User {
    private Integer id;
    private String name;
    private Short score;
    private String vector;

    private byte[] vectorBytes;

    public User() {}

    public User(Integer id, String name, Short score, String vector) {
        this.id = id;
        this.name = name;
        this.score = score;
        this.vector = vector;
    }

    public byte[] getVectorBytes() {
        return vectorBytes;
    }

    public void setVectorBytes(byte[] vectorBytes) {
        this.vectorBytes = vectorBytes;
    }


    @JsonProperty("vector")
    public String getVector() {
        return vector;
    }

    @JsonProperty("vector")
    public void setVector(String vector) {
        this.vector = vector;
    }

    @JsonProperty("id")
    public Integer getId() {
        return id;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("score")
    public Short getScore() {
        return score;
    }

    @JsonProperty("id")
    public void setId(Integer id) {
        this.id = id;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", score=" + score +
                ", vector='" + vector + '\'' +
                '}';
    }

    @JsonProperty("score")
    public void setScore(Short score) {
        this.score = score;
    }

}
