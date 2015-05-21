package cn.edu.sjtu.zzang.wordfilter;

import java.util.Arrays;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordFilterBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1102460427557324878L;
	private String[] words = {"民主", "自由", "学潮", "规范", "国务院"};

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		long id = input.getLongByField("id");
		String msg = input.getStringByField("msg");
		if (msg != null) {
			for (int i = 0; i < words.length; i++) {
				int len = words[i].length();
				char[] filler = new char[len];
				Arrays.fill(filler, '*');
				String sfiller = String.valueOf(filler);
				for (int j = 0; j <= msg.length() - len; j++) {
					if (words[i].equals(msg.substring(j, j + len))) {
						msg = msg.substring(0, j) + sfiller + msg.substring(j + len);
						System.out.println("Found: " + msg);
					}
				}
			}
			collector.emit(new Values(id, msg));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "msg"));
	}

}
