import org.example.BinaryArrayExample;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

public class LEB128Test {
    @Test
    public void testLeb128Encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int l = BinaryArrayExample.writeUnsignedLeb128(out, 624485);
        System.out.println("l: " + l);
        for(byte b: out.toByteArray()) {
            int vv = b & 0xff;
            // 229, 142, 38
            System.out.println("v:" + vv);
        }
    }
}
