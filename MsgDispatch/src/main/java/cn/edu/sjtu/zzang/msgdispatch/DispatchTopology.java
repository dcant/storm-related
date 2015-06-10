package cn.edu.sjtu.zzang.msgdispatch;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class DispatchTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, IOException
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new DataSpout(), 1);
		builder.setBolt("dispatch", new DispatchBolt(), 1).shuffleGrouping("input");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Msg_Topology", conf, builder.createTopology());
		char flag = 'r';
		while (flag != 's') {
			flag = (char)System.in.read();
		}
		Utils.sleep(10000); // wait all the task to finish, or errors come
		cluster.killTopology("Msg_Topology");
		cluster.shutdown();
//		StormSubmitter.submitTopology("msgdispatch", conf, builder.createTopology());
	}
}
