package org.example.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class LongVersionedSerializer implements SimpleVersionedSerializer<Long> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Long obj) throws IOException {
        byte[] bs = new byte[8];
        long v = obj;
        for(int i = 0; i < 8; i++) {
            bs[i] = (byte) (v >>> (8 * (7 - i)));
        }
        return bs;
    }

    @Override
    public Long deserialize(int version, byte[] serialized) throws IOException {
        if (version != 1 || serialized.length != 8) {
            throw new IOException("Invalid version or byte length for LongVersionedSerializer");
        }

        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) | (serialized[i] & 0xFF);
        }

        return value;
    }
}
