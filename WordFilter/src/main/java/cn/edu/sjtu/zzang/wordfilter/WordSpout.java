package cn.edu.sjtu.zzang.wordfilter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8234650806265597535L;
//	private final static String CHANNEL = "msg";
//    private Jedis jedis;
//    private String host;
//    private int port;
//    private SpoutOutputCollector collector;
//	
//	public void open(Map conf, TopologyContext context,
//			SpoutOutputCollector collector) {
//		// TODO Auto-generated method stub
//		this.host = conf.get(Conf.REDIS_HOST_KEY).toString();
//		this.port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
//		this.collector = collector;
//		connectToRedis();
//	}
//
//	public void nextTuple() {
//		// TODO Auto-generated method stub
//		String contentString = jedis.rpop(CHANNEL);
//		if (contentString == null || "nil".equals(contentString)) {
//			try {
//				Thread.sleep(500);
//			} catch (Exception e) {
//				// TODO: handle exception
//			}
//		} else {
//			String[] splitStrings = contentString.split("#`#");
//			long id = Long.valueOf(splitStrings[0]);
//			String msg = splitStrings[1];
//			collector.emit(new Values(id, msg));
//		}
//	}
//
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		// TODO Auto-generated method stub
//		declarer.declare(new Fields("id", "msg"));
//	}
//
//	private void connectToRedis() {
//		this.jedis = new Jedis(host, port);
//	}

	private SpoutOutputCollector collector;
	private Connection connection = null;
	private Statement st = null;
	private String tablename = "information";
	private long id = 0;
	
	private void LinkDB() {
		// TODO Auto-generated method stub
		String url = "jdbc:mysql://" + Conf.DB_HOST + "/"
				+ Conf.DB_NAME + "?useUnicode=true&characterEncoding=UTF-8";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(url, Conf.DB_USER, Conf.DB_PASS);
			System.out.println("connection !!!");
			if (connection != null)
				System.out.println("connection success!");
			else
				System.out.println("connection failed!");
			st = connection.createStatement();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		LinkDB();
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			if (connection == null) {
				
				return;
			}
			
			ResultSet rSet = st.executeQuery("select information_id, content from " 
			+ tablename + " where information_id > " + id + " order by information_id asc limit 1");
			if (rSet.next()) {
				id = rSet.getLong("information_id");
				String msg = rSet.getString("content");
				System.out.println(msg);
				collector.emit(new Values(id, msg));
			} else {
				Utils.sleep(100);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "msg"));
	}
}
