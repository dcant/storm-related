package cn.edu.sjtu.zzang.msgdispatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.HttpURLConnection;
import java.util.Map;



public class URLUtil {
	private static String prepareParam(Map<String, Object> param) {
		StringBuffer sb = new StringBuffer();
/*		if (param.isEmpty()) {
			return "";
		} else {
			for (String key : param.keySet()) {
				long value = (Long)param.get(key);
				if (sb.length() < 1)
					sb.append(key).append("=").append(value);
				else
					sb.append("&").append(key).append("=").append(value);
			}
		}*/
		
		if (param.isEmpty()) {
			return "";
		} else {
			sb.append('{');
			for (String key : param.keySet()) {
				long value = (Long)param.get(key);
				if (sb.length() < 2)
					sb.append('"' + key + '"' + ':').append(value);
				else {
					sb.append(',').append('"').append(key).append('"').append(':').append(value);
				}
			}
			sb.append('}');
		}
		
		return sb.toString();
	}
	
	public static String util_post(String urlstr, Map<String, Object> param) throws IOException {
		String paramStr = prepareParam(param);
		URL url = new URL(urlstr);
		HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		
		connection.setRequestMethod("POST");
		connection.setDoInput(true);
		connection.setDoOutput(true);
		connection.setRequestProperty("Content-Type", "application/json");
		
		OutputStream os = connection.getOutputStream();
		os.write(paramStr.toString().getBytes("utf-8"));
		os.flush();
		os.close();
		
		BufferedReader buf = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String line;
		String result = "";
		while ((line = buf.readLine()) != null) {
			result += "\n" + line;
		}
		buf.close();
		return result;
	}
}
