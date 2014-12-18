package cn.edu.sjtu.dclab.zzang.topology;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitterBolt extends BaseBasicBolt {
	public SplitterBolt() {
		
	}
	@Override
	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		String line = arg0.getStringByField("line");
		String[] split = line.split(" ");
		for (String word : split) {
			word = word.trim();
			if (!word.isEmpty()) {
				word.toLowerCase();
				arg1.emit(new Values(word));
			}
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("word"));
	}
}
