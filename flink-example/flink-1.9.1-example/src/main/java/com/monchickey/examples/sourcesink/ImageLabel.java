package com.monchickey.examples.sourcesink;

import com.alibaba.fastjson.annotation.JSONField;

public class ImageLabel {
    @Override
    public String toString() {
        return "ImageLabel{" +
                "labelId=" + labelId +
                ", labelName='" + labelName + '\'' +
                '}';
    }

    @JSONField(name = "label_id")
    public int labelId;
    @JSONField(name = "label_name")
    public String labelName;
}
