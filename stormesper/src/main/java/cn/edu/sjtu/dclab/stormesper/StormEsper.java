package cn.edu.sjtu.dclab.stormesper;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;


import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

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




public class StormEsper {
	public static void main(String[] args) throws IOException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new DataSpout(), 10);
		builder.setBolt("match", new MatchBolt(), 10).shuffleGrouping("input");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormEsper", conf, builder.createTopology());
		char flag = 'r';
		while (flag != 's') {
			flag = (char)System.in.read();
		}
		Utils.sleep(10000); // wait all the task to finish, or errors come
		cluster.killTopology("StormEsper");
		cluster.shutdown();
	}
	
	private static class DataSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;
		public void nextTuple() {
			// TODO Auto-generated method stub
			Utils.sleep(1000);
			int price = RandomUtils.nextInt(10);
			collector.emit(new Values(price));
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

		EPRuntime cepRT;
		
		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub
			int price = arg0.getIntegerByField("msg");
	        long timeStamp = System.currentTimeMillis();  
	        String symbol = "AAPL";  
	        Tick tick = new Tick(symbol, price, timeStamp);  
	        System.out.println("Sending tick:" + tick);  
	        cepRT.sendEvent(tick);
		}

		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
	        Configuration cepConfig = new Configuration();  
	        cepConfig.addEventType("StockTick", Tick.class.getName());  
	        EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);  
	        cepRT = cep.getEPRuntime();  
	   
	        EPAdministrator cepAdm = cep.getEPAdministrator();  
	        EPStatement cepStatement = cepAdm.createEPL("select * from " +  
	                "StockTick(symbol='AAPL').win:length(2) " +  
	                "having avg(price) > 6.0");  
	   
	        cepStatement.addListener(new CEPListener());
		}

		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	
    public static class CEPListener implements UpdateListener {  
    	   
        public void update(EventBean[] newData, EventBean[] oldData) {  
            System.out.println("Event received: " + newData[0].getUnderlying());
        }  
    }  
	
    public static class Tick {  
        String symbol;  
        Double price;  
        Date timeStamp;  
   
        public Tick(String s, double p, long t) {  
            symbol = s;  
            price = p;  
            timeStamp = new Date(t);  
        }  
        public double getPrice() {return price;}  
        public String getSymbol() {return symbol;}  
        public Date getTimeStamp() {return timeStamp;}  
   
        @Override  
        public String toString() {  
            return "Price: " + price.toString() + " time: " + timeStamp.toString();  
        }  
    }  
}
