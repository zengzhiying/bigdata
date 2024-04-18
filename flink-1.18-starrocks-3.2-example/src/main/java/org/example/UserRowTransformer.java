package org.example;

import com.starrocks.connector.flink.row.sink.StarRocksSinkRowBuilder;

public class UserRowTransformer implements StarRocksSinkRowBuilder<User> {
    @Override
    public void accept(Object[] objects, User user) {
        objects[0] = user.getId();
        objects[1] = user.getName();
        objects[2] = user.getScore();
        // StarRocks connector 当前不支持二进制字段
        // objects[3] = user.getVectorBytes();
    }
}
