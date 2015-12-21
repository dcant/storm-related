package cn.edu.sjtu.dclab.stormdrools;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderErrors;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.io.ResourceFactory;
import org.drools.logger.KnowledgeRuntimeLogger;
import org.drools.logger.KnowledgeRuntimeLoggerFactory;
import org.drools.runtime.StatefulKnowledgeSession;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StormDrools {
	public static void main(String[] args) throws IOException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new DataSpout(), 10);
		builder.setBolt("match", new MatchBolt(), 10).shuffleGrouping("input");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormDrools", conf, builder.createTopology());
		char flag = 'r';
		while (flag != 's') {
			flag = (char)System.in.read();
		}
		Utils.sleep(10000); // wait all the task to finish, or errors come
		cluster.killTopology("StormDrools");
		cluster.shutdown();
	}
	
	private static class DataSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;
		public void nextTuple() {
			// TODO Auto-generated method stub
			Utils.sleep(1000);
			String[] wordsStrings = {"hello", "world", "badass", "guess"};
			String word = wordsStrings[RandomUtils.nextInt(4)];
			collector.emit(new Values(word));
		}

		public void open(Map arg0, TopologyContext arg1,
				SpoutOutputCollector arg2) {
			// TODO Auto-generated method stub
			this.collector = arg2;
		}

		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			arg0.declare(new Fields("msg"));
		}
		
	}
	
	private static class MatchBolt extends BaseRichBolt {

		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub
			String word = arg0.getStringByField("msg");
	        try {
	            // load up the knowledge base
	            KnowledgeBase kbase = readKnowledgeBase();
	            StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession();
	            KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession, "test");
	            // go !
	            Message message = new Message();
	            message.setMessage(word);
	            message.setStatus(Message.BAD);
	            ksession.insert(message);
	            ksession.fireAllRules();
	            logger.close();
	        } catch (Throwable t) {
	            t.printStackTrace();
	        }
		}

		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			
		}

		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
    private static KnowledgeBase readKnowledgeBase() throws Exception {
        KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
        kbuilder.add(ResourceFactory.newClassPathResource("Rules.drl"), ResourceType.DRL);
        KnowledgeBuilderErrors errors = kbuilder.getErrors();
        if (errors.size() > 0) {
            for (KnowledgeBuilderError error: errors) {
                System.err.println(error);
            }
            throw new IllegalArgumentException("Could not parse knowledge.");
        }
        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());
        return kbase;
    }

    public static class Message {

        public static final int BAD = 0;
        public static final int GOOD = 1;

        private String message;

        private int status;

        public String getMessage() {
            return this.message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public int getStatus() {
            return this.status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

    }
}
