package cn.edu.sjtu.zzang.msgdispatch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class Message {
	@JsonProperty(value="from")
	private int fromid;
	@JsonProperty(value="to")
	private int toid;
	@JsonProperty(value="type")
	private int type;
	@JsonProperty(value="content")
	private String content;
	@JsonProperty(value="userId")
	private long userid;
	@JsonProperty(value="name")
	private String uname;
	
	public void setFromid(int id) {
		this.fromid = id;
	}
	public int getFromid() {
		return this.fromid;
	}
	
	public void setToid(int id) {
		this.toid = id;
	}
	public int getToid() {
		return this.toid;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	public int getType() {
		return this.type;
	}
	
	public void setContent(String content) {
		this.content = content;
	}
	public String getContent() {
		return this.content;
	}
	
	public String getUname() {
		return uname;
	}
	public void setUname(String uname) {
		this.uname = uname;
	}
	
	public long getUserid() {
		return userid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}
}
