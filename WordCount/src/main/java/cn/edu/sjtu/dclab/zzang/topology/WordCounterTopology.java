package cn.edu.sjtu.dclab.zzang.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCounterTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("generator",new GeneratorSpout(), 1);
		builder.setBolt("splitter", new SplitterBolt(), 3)
			.shuffleGrouping("generator");
		builder.setBolt("counter", new CounterBolt(),1)
			.fieldsGrouping("splitter", new Fields("word"));
		
        //Configuration
		Config conf = new Config();
		conf.put("filepath", "words.dat");
		conf.setDebug(true);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Word_Counter_Topology", conf, builder.createTopology());
		Utils.sleep(10000); // wait all the task to finish, or errors come
		cluster.killTopology("Word_Counter_Topology");
		cluster.shutdown();
	}
}
