package com.monchickey.hive;

import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;

public class CustomAuthenticationProvider implements PasswdAuthenticationProvider {

    HashMap<String, byte[]> passwdStore = null;

    public CustomAuthenticationProvider() {
        passwdStore = new HashMap<>();
        passwdStore.put("hive", new byte[]{
                -110,118,54,-30,75,7,-10,7,-63,18,-49,-106,114,-35,-49,22,93,-75,-18,-121,108,-2,-99,-73,-75,-35,108,-72,107,21,90,68});
        // 多个用户继续添加
        // passwdStore.put("hive2", new byte[]{})
    }

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        AuthenticationException authException = new AuthenticationException("Authenticator Error!");
        if(user == null || password == null)
            throw authException;
        if(passwdStore.get(user) != null &&
                Arrays.equals(passwdStore.get(user),
                        SHA256Digest(password.getBytes(StandardCharsets.UTF_8))))
            return;
        throw authException;
    }

    public byte[] SHA256Digest(byte[] src) {
        byte[] d = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(src);
            d = md.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return d;
    }
}
