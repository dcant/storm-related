package cn.edu.sjtu.dclab.zzang.topology;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class GeneratorSpout extends BaseRichSpout {

	private SpoutOutputCollector spoutOutput = null;
	private FileReader freader = null;
	private boolean done = false;
	
	public GeneratorSpout() {
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("line"));
	}
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if (done) {
			Utils.sleep(1000);
			return;
		}
		Utils.sleep(100);
		String line = null;
		BufferedReader linebuffer = new BufferedReader(freader);
		try {
			while ((line = linebuffer.readLine()) != null) {
				spoutOutput.emit(new Values(line), line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			done = true;
		}
	}
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		try {
			this.freader = new FileReader(arg0.get("filepath").toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.spoutOutput = arg2;
	}
}
