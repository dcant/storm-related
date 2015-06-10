package cn.edu.sjtu.zzang.msgdispatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import cn.edu.sjtu.se.dclab.auth.thrift.AuthClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DispatchBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 272595194167901485L;
	
	private MqttClient dispatcher;
    private int qos = Conf.RABBITMQ_QOS;
    private AuthClient client;

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String msg = input.getStringByField("msg");
		msg_dispatch(msg);
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
        prepare_dispatcher();
        prepare_authclient();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}
	
	private void prepare_dispatcher() {
		String broker       = Conf.RABBITMQ_BROKER;
        String clientId     = "storm-pusher";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            dispatcher = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(Conf.RABBITMQ_USER);
            connOpts.setPassword(Conf.RABBITMQ_PASS);
            dispatcher.connect(connOpts);
            System.out.println("Bolt: Connected");

        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
	}
	
	private void prepare_authclient() {
		client = new AuthClient();
		client.setNodeName("/authService");
		try {
			client.startClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}

	private void msg_dispatch(String msg) {
		int fid = 0;
		int tid = 0;
		int type = 0;
		boolean flag = false;
		String content = "";
		String topic = "recv";
		try {
            System.out.println("Bolt message: " + msg);
            
            ObjectMapper mapper = null;
            try {
                mapper = new ObjectMapper();
                Message ms = mapper.readValue(msg, Message.class);
                fid = ms.getFromid();
                tid = ms.getToid();
                type = ms.getType();
                if (client.validation(fid, tid, type)) {
                	flag = true;
                	content += ms.getContent();
                } else {
                	flag = false;
                	content += "请先申请添加！";
                }
/*                Map<String, Object> param = new HashMap<String, Object>();
                param.put("from", fid);
                param.put("to", tid);
                param.put("type", type);
                String res = URLUtil.util_post(Conf.RELATION_URL, param).trim();
				if (res.equals("true")) {
					flag = true;
					content += ms.getContent();
				} else {
					flag = false;
					content += "请先申请添加！";
				}*/
			} catch (IOException e) {
				// TODO Auto-generated catch block
				flag = false;
				content += "获取关系失败！";
				e.printStackTrace();
			}
            Message sendingMsg = new Message();
            sendingMsg.setContent(content);
            sendingMsg.setFromid(fid);
            sendingMsg.setToid(tid);
            sendingMsg.setType(1);
            String sendingMsgStr = "";
            try {
				sendingMsgStr = mapper.writeValueAsString(sendingMsg);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
            
            MqttMessage message = new MqttMessage(sendingMsgStr.getBytes());
            message.setQos(qos);
            if (flag)
            	topic += tid;
            else
            	topic += fid;
            System.out.println("Publishing:" + topic + " " + sendingMsgStr);
            dispatcher.publish(topic, message);
		} catch (MqttPersistenceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
