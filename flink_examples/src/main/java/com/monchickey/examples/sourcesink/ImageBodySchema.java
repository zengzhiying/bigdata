package com.monchickey.examples.sourcesink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class ImageBodySchema implements DeserializationSchema<ImageBody>, SerializationSchema<ImageBody> {
    @Override
    public ImageBody deserialize(byte[] bytes) throws IOException {
        String jsonString = new String(bytes, "utf-8");
        if(JSON.isValid(jsonString)) {
            JSONObject jsonObj = JSON.parseObject(jsonString);
            ImageBody imageBody = new ImageBody();
            imageBody.imageId = jsonObj.getIntValue("image_id");
            String bodyBase64 = jsonObj.getString("image_body");
            if(Base64.isBase64(bodyBase64)) {
                imageBody.imageBody = Base64.decodeBase64(bodyBase64);
            } else {
                System.out.println("Image body base64 error: " + bodyBase64);
                imageBody.imageBody = new byte[0];
            }
            return imageBody;
        }
        System.out.println("Json校验失败！" + jsonString);
        return null;
    }

    @Override
    public boolean isEndOfStream(ImageBody imageBody) {
        return false;
    }

    @Override
    public byte[] serialize(ImageBody imageBody) {
        Map<String, Object> m = new HashMap<>();
        m.put("image_id", imageBody.imageId);
        String imageBodyBase64 = Base64.encodeBase64String(imageBody.imageBody);
        m.put("image_body", imageBodyBase64);
        return JSON.toJSONString(m).getBytes(Charset.forName("utf-8"));
    }

    @Override
    public TypeInformation<ImageBody> getProducedType() {
        return TypeInformation.of(ImageBody.class);
    }
}
