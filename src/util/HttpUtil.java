package util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONArray;
import org.json.JSONObject;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

public class HttpUtil {
	public static String send(String targetURL, String urlParameters, String method) {
		HttpURLConnection connection = null;
		try {
			URL url = new URL(targetURL);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(method);

			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
			connection.setRequestProperty("Content-Language", "en-EN");

			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);

			// Send request
			if (urlParameters != null && urlParameters.length() > 0) {
				DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
				wr.write(urlParameters.getBytes("UTF-8"));
				wr.flush();
				wr.close();
			}

			// Get Response
			InputStream is = connection.getResponseCode() >= 200 && connection.getResponseCode() < 300
					? connection.getInputStream() : connection.getErrorStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, "UTF-8"));
			String line;
			StringBuilder response = new StringBuilder();
			while ((line = rd.readLine()) != null) {
				response.append(line);
				response.append('\r');
			}
			rd.close();
			return response.toString();

		} catch (Exception e) {
			return null;

			// throw ne;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}
}
