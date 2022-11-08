package com.monchickey.examples.sourcesink;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Arrays;

public class ImageBody {
    @JSONField(name = "image_id")
    public int imageId;
    @JSONField(name = "image_body")
    public byte[] imageBody;

    @Override
    public String toString() {
        return "ImageBody{" +
                "imageId=" + imageId +
                ", imageBody=" + Arrays.toString(imageBody) +
                '}';
    }
}
