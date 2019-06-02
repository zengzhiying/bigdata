package net.zengzhiying.example1;

import java.util.ArrayList;
import java.util.List;

/**
 * kafka demo
 *
 */
public class ProducerExample
{
    public static void main( String[] args )
    {
        if(args == null || args.length == 0) {
            ProducerUtil.sendMessage("This is message");
        } else {
            List<String> msgList = new ArrayList<String>();
            
            for(int i = 1; i < 100; i++) {
                msgList.add("message number : " + i);
            }
            ProducerUtil.sendBatchMessage(msgList);
        }
    }
}
