package cn.edu.sjtu.zzang.wordfilter;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class FilterTopology {
//	private TopologyBuilder builder = new TopologyBuilder();
//	private Config conf = new Config();
//	private LocalCluster cluster;
//
//	public FilterTopology() {
//		builder.setSpout("wordspout", new WordSpout(), 10);
//
//		builder.setBolt("wordfilter", new WordFilterBolt(), 10).shuffleGrouping("wordspout");
//		builder.setBolt("update", new DbUpdateBolt(), 10).shuffleGrouping("wordfilter");
//
//		conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);
//	}
//
//	public TopologyBuilder getBuilder() {
//		return builder;
//	}
//
//	public LocalCluster getLocalCluster() {
//		return cluster;
//	}
//
//	public Config getConf() {
//		return conf;
//	}
//
//	public void runLocal(int runTime) {
//		conf.setDebug(true);
//		conf.put(Conf.REDIS_HOST_KEY, "localhost");
//		cluster = new LocalCluster();
//		cluster.submitTopology("test", conf, builder.createTopology());
//		if (runTime > 0) {
//			Utils.sleep(runTime);
//			shutDownLocal();
//		}
//	}
//
//	public void shutDownLocal() {
//		if (cluster != null) {
//			cluster.killTopology("test");
//			cluster.shutdown();
//		}
//	}
//
//	public void runCluster(String name, String redisHost)
//			throws AlreadyAliveException, InvalidTopologyException {
//		conf.setNumWorkers(20);
//		conf.put(Conf.REDIS_HOST_KEY, redisHost);
//		StormSubmitter.submitTopology(name, conf, builder.createTopology());
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		FilterTopology topology = new FilterTopology();
//
//		if (args != null && args.length > 1) {
//			topology.runCluster(args[0], args[1]);
//		} else {
//			if (args != null && args.length == 1)
//				System.out.println("Running in local mode, redis ip missing for cluster run");
//			topology.runLocal(10000);
//		}
//
//	}
	
	public static void main(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("generator",new WordSpout(), 1);
		builder.setBolt("filter", new WordFilterBolt(), 3)
			.shuffleGrouping("generator");
		builder.setBolt("updater", new DbUpdateBolt(),1)
			.shuffleGrouping("filter");
		

		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

/*		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Word_Filter_Topology", conf, builder.createTopology());
		char flag = 'r';
		while (flag != 's') {
			flag = (char)System.in.read();
		}
		Utils.sleep(10000); // wait all the task to finish, or errors come
		cluster.killTopology("Word_Filter_Topology");
		cluster.shutdown();*/
		StormSubmitter.submitTopology("wordfilter", conf, builder.createTopology());
	}
}