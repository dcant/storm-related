package cn.edu.sjtu.dclab.zzang.topology;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class CounterBolt extends BaseRichBolt {

	private String name;
	private int id;
	HashMap<String, Integer> counter = new HashMap<String, Integer>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("res"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		super.cleanup();
		System.out.println("=== Word Counter [" + name + ":" + id + "]");
		for (Map.Entry<String, Integer> entry : counter.entrySet()) {
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		name = context.getThisComponentId();
		id = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String word = input.getStringByField("word");
		if (counter.containsKey(word)) {
			counter.put(word, counter.get(word) + 1);
		} else
			counter.put(word, 1);
	}

}
