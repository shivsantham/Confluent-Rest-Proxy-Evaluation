package io.confluent.examples.producer;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.parser.JSONParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import io.confluent.examples.producer.JSONUtil;

public class RestProducerThread implements Runnable {

    private static String topics = "topics";
    private String topic;
    private String host;
    private int port;
    private String url;
    private JSONObject message;
    private int noOfMessages;
    static Object lock = new Object();
    private static Map<String,Long> topicMap = new HashMap<String,Long>();
    Date date;

    RestProducerThread(String topic, String host,int port, 
            JSONObject message, int noOfContinousMessages) {
        this.topic = topic;
        this.host = host;
        this.port = port;
        this.message = message;
        noOfMessages = noOfContinousMessages;
        date = new Date();
        
        synchronized (lock) {
            if(topicMap.get(topic) == null) {
               topicMap.put(topic,(long) 0);
            }
        }
    }

  private String buildAndGetUrl(String topic,String host, int port) {

       StringBuffer buf = new StringBuffer();
          buf.append("http://");
          buf.append(host);
          buf.append(":");
          buf.append(port);
          buf.append("/");
          buf.append(topics);
          buf.append("/");
          buf.append(topic);
          
       return buf.toString();
    }

    public void run() {
       
       /* Each message has to be sent through 
        * a Http POST when producing messages through
        * Confluent Rest Proxy server. So for publishing 
        * N messages, N http posts needs to be done */
        postToKafka();
    }

    public void postToKafka () {

       Long startThreadTime = System.currentTimeMillis();
       for (int i=0; i<noOfMessages;i++) {
            pushDataKafka(buildAndGetUrl
                      (topic,host,port),message);
               synchronized(lock) {
                    topicMap.put(topic,topicMap.get(topic) + 1);
               }
       }
       long produceThreadTime = System.currentTimeMillis();
       long elapsedTime = produceThreadTime - startThreadTime;
          
          /*System.out.println(new Timestamp(date.getTime()) +
                     " :] Time taken to publish " + message.length() +
                     " characters ::" + noOfMessages +":: times on Topic "+ topic+ 
                     "for producer Thread" + Thread.currentThread() + 
                     " ::: " + elapsedTime + " milli seconds");*/
          
          RestProducerGroup.totalTimeProducing += elapsedTime;
    }
    
    public static long getTopicCount (String topic) {
        synchronized(lock) {
          return topicMap.get(topic);
        }
    }

    public static String pushDataKafka (String url, JSONObject data) {
       return httpPostRequest(url,data);
    }

    static String httpPostRequest (String url, JSONObject obj)
    {
        
        StringBuilder sb = new StringBuilder();
        try { 
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection)object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", 
                     "application/vnd.kafka.json.v1+json");
            con.setRequestProperty("Accept", "application/vnd.kafka.v1+json, "
                     + "application/vnd.kafka+json, application/json");
            /*
            Keeping the http connections alive, opening up a new 
            connection while posting new messages to Rest Proxy
            takes an extra overhead
            */
            con.setRequestProperty("Connection", "Keep-Alive");
            con.setRequestProperty("Keep-Alive", "header");
            con.setRequestMethod("POST");
            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream());
            wr.write(obj.toString());
            wr.flush();

            int HttpResult = con.getResponseCode(); 
            if (HttpResult == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader
                        (con.getInputStream(),"utf-8"));
                String line = null;  
                while ((line = br.readLine()) != null) {  
                    sb.append(line + "\n");  
                }
                br.close();
            }
        }
        catch (IOException  e) {
            throw new IllegalArgumentException("Unable to retrieve data from kafka "
                   + "Rest proxy, Content type mismatch");
        }
        return sb.toString();
    }

}
