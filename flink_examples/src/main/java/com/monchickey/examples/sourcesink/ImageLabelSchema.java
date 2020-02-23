package com.monchickey.examples.sourcesink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

public class ImageLabelSchema implements DeserializationSchema<ImageLabel>, SerializationSchema<ImageLabel> {
    @Override
    public ImageLabel deserialize(byte[] bytes) throws IOException {
        String jsonString = new String(bytes, "utf-8");
        if(JSON.isValid(jsonString)) {
            return JSON.parseObject(jsonString, ImageLabel.class);
        }
        System.out.println("Json校验失败！" + jsonString);
        return null;
    }

    @Override
    public boolean isEndOfStream(ImageLabel imageLabel) {
        return false;
    }

    @Override
    public byte[] serialize(ImageLabel imageLabel) {
        return JSON.toJSONString(imageLabel).getBytes(Charset.forName("utf-8"));
    }

    @Override
    public TypeInformation<ImageLabel> getProducedType() {
        return TypeInformation.of(ImageLabel.class);
    }
}
