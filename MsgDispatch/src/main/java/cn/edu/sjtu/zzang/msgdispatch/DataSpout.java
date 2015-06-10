package cn.edu.sjtu.zzang.msgdispatch;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DataSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1506556289916628876L;
	private SpoutOutputCollector collector;
	private boolean flag = true;
	private MqttClient consumer;
	private Queue<String> mQueue = new LinkedList<String>();

	public void nextTuple() {
		// TODO Auto-generated method stub
		if (flag){ 
			if (!mQueue.isEmpty()) {
				String msg = mQueue.poll();
				System.out.println(msg);
				collector.emit(new Values(msg));
				flag = true;
			} else {
				flag = false;
			}
		} else {
			Utils.sleep(300);
			flag = true;
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		prepare_consumer();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("msg"));
	}

	private void prepare_consumer() {
        String topic        = Conf.RABBITMQ_TOPIC;
        int qos             = Conf.RABBITMQ_QOS;
        String broker       = Conf.RABBITMQ_BROKER;
        String clientId     = "storm-consumer";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            consumer = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(Conf.RABBITMQ_USER);
            connOpts.setPassword(Conf.RABBITMQ_PASS);
            connOpts.setKeepAliveInterval(5);
            consumer.connect(connOpts);
            System.out.println("Spout: Connected");
            
            consumer.setCallback(new MqttCallback() {
				
				public void messageArrived(String topic, MqttMessage msg) throws Exception {
					// TODO Auto-generated method stub
					mQueue.add(new String(msg.getPayload()));
					System.out.println("Spoutr:" + new String(msg.getPayload()));
				}
				
				public void deliveryComplete(IMqttDeliveryToken token) {
					// TODO Auto-generated method stub
					
				}
				
				public void connectionLost(Throwable cause) {
					// TODO Auto-generated method stub
					
				}
			});
            
            consumer.subscribe(topic, qos);
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
	}
}
