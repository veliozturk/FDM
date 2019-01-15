package here;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import util.GenericUtil;
import util.HttpUtil;
import util.RedisUtil;

public class FetchDistanceMatrix {
	public static String rabbitHost = "35.239.184.252"; //rabbitmq
	public static String rabbitUsername = "guest";
	public static String rabbitPassword = "guest";
	
	public static String fetchQueue = "optiyol-scenario-fetch";
	public static String resultQueue = "optiyol-scenario-result";
	public static String errorQueue = "optiyol-scenario-error";
	public static String callbackQueue = "optiyol-scenario-callback";
	public static int threadCount = 10;
	public static int retryCount = 5;
	public static int successCount = 0;
	public static int errorCount = 0;
	public static int requestCount = 0;
	public static long totalTime = 0;
	public static long restTime = 0;

	public static String redisHost = "35.226.30.186";//redis
	public static String redisUsername = null;
	public static String redisPassword = null;
	
	public static Map paramMap = new HashMap();
	public static boolean debug = false;

	public Consumer getDefaultConsumer(Channel channel, String threadId) {
		System.out.println(" [FETCH] Waiting for messages. " + threadId);
		return new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				Set<String> locPairKeyz = new HashSet<String>();
				Set<Integer> startErrorKeyz = new HashSet<Integer>();
				Set<Integer> destinationErrorKeyz = new HashSet<Integer>();
				Map<Integer, String> startMap = new HashMap<Integer, String>();
				Map<Integer, String> destinationMap = new HashMap<Integer, String>();
				Map<String, String> resultMap = new HashMap();
				requestCount++;
				int reqNum = requestCount;
				try {
					long startTime = System.currentTimeMillis();
					String message = new String(body, "UTF-8"); //scenarioId:[startLocationId:lat,lon]x[destinationLocationId:lat,lon]
					if(message.startsWith("icb-kill")){
						channel.basicPublish("", fetchQueue, null, message.getBytes("UTF-8"));
						channel.basicCancel(consumerTag);
						return;
					}
					if(debug) System.out.println(threadId + " [received: "+message.length()+"] '" + message.substring(0,50)+ "...'");
					int ix = message.indexOf(':');
					int scenarioId = new Integer(message.substring(0, ix));
					message = message.substring(ix + 1);
					StringBuilder s = new StringBuilder();
					s.append(
							"https://matrix.route.api.here.com/routing/7.2/calculatematrix.json?app_id=cJIfZxnGkVefFPXssC6j&app_code=jTs-18XfJ728hC2PWS-EYg&mode=shortest;car;traffic:enabled;boatFerry:-3&summaryAttributes=traveltime,distance");

					ix = message.indexOf('x');
					String[] starts = message.substring(0, ix).split(";");
					String[] destinations = message.substring(ix + 1).split(";");
					for (int qi = 0; qi < starts.length; qi++) {
						ix = starts[qi].indexOf(':');
						String loc = starts[qi].substring(ix + 1);
						startMap.put(qi, starts[qi].substring(0, ix));
						s.append("&start").append(qi).append("=").append(loc);
						for (int zi = 0; zi < destinations.length; zi++) {
							locPairKeyz.add(qi + "-" + zi);
						}

					}
					successCount += locPairKeyz.size();
					
					for (int qi = 0; qi < destinations.length; qi++) {
						ix = destinations[qi].indexOf(':');
						String loc = destinations[qi].substring(ix + 1);
						destinationMap.put(qi, destinations[qi].substring(0, ix));
						s.append("&destination").append(qi).append("=").append(loc);
					}
					long startTime2 = System.currentTimeMillis();
					String r = HttpUtil.send(s.toString(), null, "GET");
					startTime2 = System.currentTimeMillis() - startTime2;
					if (r != null && r.length() > 0) {
						JSONObject json = new JSONObject(r);
						if (json.has("response")) {
							JSONObject response = json.getJSONObject("response");
							if (response.has("matrixEntry")) {
								JSONArray ar = response.getJSONArray("matrixEntry");
								for (int qi = 0; qi < ar.length(); qi++) {
									JSONObject jo = ar.getJSONObject(qi);
									if (jo.has("summary")) {
										JSONObject su = jo.getJSONObject("summary");
										if (su.has("travelTime") && su.has("distance") && su.has("costFactor")) {
											String startId = startMap.get(jo.getInt("startIndex"));
											String destinationId = destinationMap.get(jo.getInt("destinationIndex"));
											if (startId != null && destinationId != null) {
												resultMap.put(startId + "-" + destinationId, su.getInt("travelTime") + ":" + su.getInt("distance") + ":" + su.getInt("costFactor"));
												locPairKeyz.remove(
														jo.get("startIndex") + "-" + jo.get("destinationIndex"));
											}/* else {
												startErrorKeyz.add(jo.getInt("startIndex"));
												destinationErrorKeyz.add(jo.getInt("destinationIndex"));
											} */
										}
									}
								}
							}
						}
					}

					for(int rc=0;!locPairKeyz.isEmpty() && rc<retryCount;rc++)try{ //error retry
						if(debug)System.out.println(reqNum + ".MQ Retry " + (rc+1));
						startErrorKeyz.clear();destinationErrorKeyz.clear();
						for(String key:locPairKeyz){
							String[] kk = key.split("-");
							startErrorKeyz.add(new Integer(kk[0]));
							destinationErrorKeyz.add(new Integer(kk[1]));
						}
						
						s.setLength(0);
						s.append("https://matrix.route.api.here.com/routing/7.2/calculatematrix.json?app_id=cJIfZxnGkVefFPXssC6j&app_code=jTs-18XfJ728hC2PWS-EYg&mode=balanced;car;traffic:enabled;boatFerry:-3&summaryAttributes=traveltime,distance");
						int cnt=0;
						
						int[] startErrorKeyMap = new int[startErrorKeyz.size()];
						int[] destinationErrorKeyMap = new int[destinationErrorKeyz.size()];
						
						for(Integer qi:startErrorKeyz){
							ix = starts[qi].indexOf(':');
							String loc = starts[qi].substring(ix + 1);
							s.append("&start").append(cnt).append("=").append(loc);
							startErrorKeyMap[cnt++] = qi;
						}
						cnt=0;
						for(Integer qi:destinationErrorKeyz){
							ix = destinations[qi].indexOf(':');
							String loc = destinations[qi].substring(ix + 1);
							s.append("&destination").append(cnt).append("=").append(loc);
							destinationErrorKeyMap[cnt++] = qi;
						}
						r = HttpUtil.send(s.toString(), null, "GET");
						
						if (r != null && r.length() > 0) {
							JSONObject json = new JSONObject(r);
							if (json.has("response")) {
								JSONObject response = json.getJSONObject("response");
								if (response.has("matrixEntry")) {
									JSONArray ar = response.getJSONArray("matrixEntry");
									startErrorKeyz.clear();destinationErrorKeyz.clear();
									for (int qi = 0; qi < ar.length(); qi++) {
										JSONObject jo = ar.getJSONObject(qi);
										if (jo.has("summary")) {
											JSONObject su = jo.getJSONObject("summary");
											if (su.has("travelTime") && su.has("distance") && su.has("costFactor")) {
												int startIndex = startErrorKeyMap[jo.getInt("startIndex")];
												int destinationIndex = destinationErrorKeyMap[jo.getInt("destinationIndex")];
												String startId = startMap.get(startIndex);
												String destinationId = destinationMap.get(destinationIndex);
												if (startId != null && destinationId != null) {
													resultMap.put(startId + "-" + destinationId, su.getInt("travelTime") + ":" + su.getInt("distance") + ":" + su.getInt("costFactor"));
													locPairKeyz.remove(startIndex + "-" + destinationIndex);
												}/* else {
													startErrorKeyz.add(startIndex);
													destinationErrorKeyz.add(destinationIndex);
												}*/
											}
										}
									}
								}
							}
						}
						if(locPairKeyz.isEmpty()){
							System.out.println(reqNum + ".MQ Retry " + (rc+1) + " Success!");
						}
						
					} catch(Exception ee){
//						if(debug)ee.printStackTrace();
						System.err.println("MQ Retry Error: " + ee.getMessage());
					}


					//RESULT PREPARE
					StringBuilder msg = new StringBuilder(50*destinations.length*starts.length);
					msg.append(scenarioId).append(":").append(resultMap.size()).append(":s");//scenarioId:[startLocationId-destinationLocationId=travelTime:distance,costFactor];
					for (String key : resultMap.keySet()) {
						msg.append(key).append("=").append(resultMap.get(key)).append(";");
					}

					totalTime += (System.currentTimeMillis()-startTime);
					restTime += startTime2;

					errorCount += locPairKeyz.size();
					if(msg.charAt(msg.length()-1)==';'){
						msg.setLength(msg.length()-1);
						if(!GenericUtil.isEmpty(resultQueue))
							channel.basicPublish("", resultQueue, null, msg.toString().getBytes("UTF-8"));
						if(!GenericUtil.isEmpty(redisHost))try{
							long rlen = RedisUtil.rpush(redisHost, fetchQueue, msg.toString());
							if(debug)System.out.println(reqNum+". "+" Redis " + rlen);
						}catch(Exception re){
							System.err.println("Redis Error " + re.getMessage());
						}
						System.out.println(reqNum+". "+threadId+"/"+scenarioId + " [success: "+(System.currentTimeMillis()-startTime)+"ms / "+startTime2+"ms / " +msg.length()+ "b] "+successCount+" / "+errorCount+" : AVGS: "+ (totalTime/requestCount) + ":" + (restTime/requestCount) +" '" + msg.toString().substring(0, 30) +"...'");
					}
					
					//ERROR PREPARE
					msg.setLength(0);
					msg.append(scenarioId).append(":").append(locPairKeyz.size()).append(":e");//scenarioId:[startLocationId-destinationLocationId];
					for (String key : locPairKeyz) {
						ix = key.indexOf('-');
						String startId = startMap.get(new Integer(key.substring(0, ix)));
						String destinationId = destinationMap.get(new Integer(key.substring(ix+1)));
						msg.append(startId).append("-").append(destinationId).append(";");
					}
					if(msg.charAt(msg.length()-1)==';'){
						msg.setLength(msg.length()-1);
						if(!GenericUtil.isEmpty(resultQueue))
						channel.basicPublish("", resultQueue, null, msg.toString().getBytes("UTF-8"));
						if(!GenericUtil.isEmpty(redisHost))try{
							long rlen = RedisUtil.rpush(redisHost, fetchQueue, msg.toString());
							if(debug)System.out.println(reqNum+". "+" Redis ERROR " + rlen);
						}catch(Exception re){
							System.err.println("Redis Error " + re.getMessage());
						}
						System.err.println(reqNum+". "+threadId+"/"+scenarioId + " [error] '" + msg.toString().substring(0, 30) +"...'");
					}

					// System.out.println(threadId+"/"+scenarioId + " [s] '" +
					// s.toString() + "'");
					//System.out.println(threadId + "/" + scenarioId + " [" + locPairKeyz.size() + "] " + r);
				} catch (Exception e) {
					System.err.println("MQ Error: " + e.getMessage());
				}

			}
		};
	}
	
	public static int getCount(){
		return 0;
	}

	public static void main(String[] args) throws IOException, TimeoutException {
/*		if (args != null && args.length > 0) {
			threadCount = new Integer(args[0]);
			if (args.length > 1) {
				rabbitHost = args[1];
			}
			if (args.length > 2) {
				fetchQueueName = args[2];
			}
			if (args.length > 3) {
				resultQueueName = args[3];
			}
			if (args.length > 4) {
				errorQueueName = args[4];
			}
		} */
		
		if (args != null && args.length > 0) {
			for (int qi = 0; qi < args.length; qi++) {
				String[] kv = args[qi].replace('=', ',').split(",");
				if (kv.length > 1) {
					if (kv[0].equals("redis"))
						redisHost = kv[1];
					else if (kv[0].equals("rabbit"))
						rabbitHost = kv[1];
					else if (kv[0].equals("threadCount"))
						threadCount = Integer.parseInt(kv[1]);
					else if (kv[0].equals("fetchQueue"))
						fetchQueue = kv[1];
					else if (kv[0].equals("resultQueue"))
						resultQueue = kv[1];
					else if (kv[0].equals("callbackQueue"))
						callbackQueue = kv[1];
					else if (kv[0].equals("errorQueue"))
						errorQueue = kv[1];
					else if (kv[0].equals("redisUsername"))
						redisUsername = kv[1];
					else if (kv[0].equals("redisPassword"))
						redisPassword = kv[1];
					else if (kv[0].equals("rabbitUsername"))
						rabbitUsername = kv[1];
					else if (kv[0].equals("rabbitPassword"))
						rabbitPassword = kv[1];
					paramMap.put(kv[0], kv[1]);
				} else
					paramMap.put(kv[0], "1");
			}
		}
		debug = paramMap.containsKey("debug");

		System.out.println("MQ thread count / host / fetch queue / result queue / error queue");
		System.out.println(threadCount + " / " + rabbitHost + " / " + fetchQueue + " / " + resultQueue + " / " + errorQueue);


		FetchDistanceMatrix fdm = new FetchDistanceMatrix();
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(rabbitHost);
		if(rabbitUsername!=null){
			factory.setUsername(rabbitUsername);
			factory.setPassword(rabbitPassword);
		}

		if (threadCount > 1)
			for (int qi = 1; qi < threadCount; qi++) {
				String threadId = "thread-" + qi;
				Thread thread = new Thread(threadId) {
					public void run() {
						try {
							Connection connection = factory.newConnection();
							Channel channel = connection.createChannel();

							channel.queueDeclare(fetchQueue, false, false, false, null);
							channel.queueDeclare(resultQueue, false, false, false, null);
							channel.queueDeclare(errorQueue, false, false, false, null);
							Consumer consumer = new FetchDistanceMatrix().getDefaultConsumer(channel, threadId);
							channel.basicConsume(fetchQueue, true, consumer);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				thread.start();
			}
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(fetchQueue, false, false, false, null);
		channel.queueDeclare(resultQueue, false, false, false, null);
		channel.queueDeclare(errorQueue, false, false, false, null);

		Consumer consumer = fdm.getDefaultConsumer(channel, "thread-0");
		channel.basicConsume(fetchQueue, true, consumer);

	}


}
