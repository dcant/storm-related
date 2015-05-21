package cn.edu.sjtu.zzang.wordfilter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DbUpdateBolt extends BaseRichBolt {

	private Connection connection = null;
	private String tablename = "information";
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		LinkDB();
	}

	private void LinkDB() {
		// TODO Auto-generated method stub
		String url = "jdbc:mysql://" + Conf.DB_HOST + "/"
				+ Conf.DB_NAME + "?useUnicode=true&characterEncoding=UTF-8";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(url, Conf.DB_USER, Conf.DB_PASS);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		long id = input.getLongByField("id");
		String msg = input.getStringByField("msg");
		UpdateDB(id, msg);
	}

	private void UpdateDB(long id, String msg) {
		// TODO Auto-generated method stub
		String sql = "update " + tablename + " t set content = '" + msg + "' where t.information_id = " + id;
		try {
			Statement statement = connection.createStatement();
			statement.executeUpdate(sql);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
