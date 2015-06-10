package cn.edu.sjtu.zzang.msgdispatch;

public class Conf {
	public static String RABBITMQ_BROKER = "tcp://192.168.1.254:1883";
	public static String RABBITMQ_TOPIC = "upload";
	public static String RABBITMQ_USER = "test";
	public static char[] RABBITMQ_PASS = "test".toCharArray();
	public static int RABBITMQ_QOS = 1;
	public static String RABBITMQ_PREFIX = "recv";
	public static String RELATION_URL = "http://202.120.40.111:8080/community-server/rest/validation/relation";
}
