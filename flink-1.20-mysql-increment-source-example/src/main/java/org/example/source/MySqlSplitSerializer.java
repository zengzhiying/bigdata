package org.example.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class MySqlSplitSerializer implements SimpleVersionedSerializer<MySqlSplit> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(MySqlSplit obj) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(outputStream);
        stream.writeInt(obj.splitId().length());
        stream.writeBytes(obj.splitId());
        stream.writeLong(obj.getStartId());
        stream.writeLong(obj.getEndId());
        stream.close();
        return outputStream.toByteArray();
    }

    @Override
    public MySqlSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != 1) {
            throw new IOException("Unsupported version: " + version);
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(byteArrayInputStream);
        int splitIdLen = in.readInt();
        byte[] splitIdBytes = new byte[splitIdLen];
        int readSize = in.read(splitIdBytes);
        if(readSize != splitIdLen) {
            throw new IOException(String.format("Read splitId len %d != %d", readSize, splitIdLen));
        }
        long startId = in.readLong();
        long endId = in.readLong();
        in.close();
        return new MySqlSplit(new String(splitIdBytes), startId, endId);
    }
}
