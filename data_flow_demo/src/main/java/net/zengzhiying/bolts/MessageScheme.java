package net.zengzhiying.bolts;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme { 
    
	private static final long serialVersionUID = 1L;


	/* (non-Javadoc)
     * @see backtype.storm.spout.Scheme#deserialize(byte[])
     */
    public List<Object> deserialize(byte[] ser) {
        try {
            String msg = new String(ser, "UTF-8"); 
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
        	e.printStackTrace();
        }
        return null;
    }
    
    
    /* (non-Javadoc)
     * @see backtype.storm.spout.Scheme#getOutputFields()
     */
    public Fields getOutputFields() {
        return new Fields("msg");  
    }  
}