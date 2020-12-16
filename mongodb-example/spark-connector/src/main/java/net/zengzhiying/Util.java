package net.zengzhiying;

public class Util {
    public static void byteToNum(byte[] b) {
        for(byte bb : b) {
            System.out.print(bb & 0xff);
            System.out.print(" ");
        }
        System.out.println();
    }
}
