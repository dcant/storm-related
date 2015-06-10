package cn.edu.sjtu.se.dclab.auth.zookeeper;

import cn.edu.sjtu.se.dclab.service_management.Content;

public class ASContent extends Content {
	private static final long serialVersionUID = -1276444776115600552L;
	
	private String ip;
	private int port;
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	
	public ASContent(String ip, int port){
		this.ip = ip;
		this.port = port;
	}
	
	@Override
	public String getStr() {
		// TODO Auto-generated method stub
		return null;
	}
}
