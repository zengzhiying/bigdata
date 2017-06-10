package net.zengzhiying.bolts;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme {

	private static final long serialVersionUID = 1L;

	public List<Object> deserialize(byte[] outputField) {
		try {
			String msg = new String(outputField, "UTF-8");
			return new Values(msg);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("msg");
	}

}
