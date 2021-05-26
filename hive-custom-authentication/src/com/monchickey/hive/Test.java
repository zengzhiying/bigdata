package com.monchickey.hive;

import javax.security.sasl.AuthenticationException;
import java.nio.charset.StandardCharsets;

public class Test {
    public static void main(String[] args) throws AuthenticationException {
        CustomAuthenticationProvider cap = new CustomAuthenticationProvider();
        byte[] b = cap.SHA256Digest("hive123456".getBytes(StandardCharsets.UTF_8));
        for(byte bb : b) {
            System.out.print(bb + ",");
        }
        System.out.println();

        cap.Authenticate("hive","hive123456");
    }
}
