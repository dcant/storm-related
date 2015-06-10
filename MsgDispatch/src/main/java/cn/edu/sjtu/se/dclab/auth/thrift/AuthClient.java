package cn.edu.sjtu.se.dclab.auth.thrift;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.json.JSONObject;

import cn.edu.sjtu.se.dclab.auth.zookeeper.ASContent;
import cn.edu.sjtu.se.dclab.service_management.Content;
import cn.edu.sjtu.se.dclab.service_management.ServiceManager;

public class AuthClient {
	private String serverIp;
	private int port;
	private Auth.Client client;
	private String nodeName;
	
	
	public void startClient() throws Exception {
		ServiceManager manager = ServiceManager.getInstance();
		List<String> contents = manager.retrieve(nodeName);
		if (contents.size() == 0) {
			throw new Exception("cannot start");
		}
		String content = (String) contents.get(0);
		String[] parts = content.split(","); 
		if (parts.length < 2) {
			throw new Exception("content wrong " + content);
		}
		
		serverIp = parts[0];
		port = Integer.parseInt(parts[1]);
	}
	
	public Boolean validation(int from, int to, int type) {
		try {
			TSocket transport = new TSocket(serverIp, port);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);

			Auth.Client client = new Auth.Client(protocol);

			Map<String, Object> map = new HashMap<String, Object>();
			map.put("command", "validation");
			Map<String, Object> commandMap = new HashMap<String, Object>();
			commandMap.put("from", from);
			commandMap.put("to", to);
			commandMap.put("type", type);
			map.put("body", commandMap);
			 
			JSONObject jsonObject = new JSONObject(map);
			String operation = jsonObject.toString();
			Boolean result = client.hasAuthority(0, operation);
			System.out.println("Return from server: " + result);
			transport.close();
			return result;
		} catch (TException e) {
			e.printStackTrace();
			return false;
		}

	}


	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public static void main(String[] args) {
		AuthClient client = new AuthClient();
		client.setNodeName("/authService");
		try {
			client.startClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		System.out.println(client.validation(1, 1, 1));
	}

}
