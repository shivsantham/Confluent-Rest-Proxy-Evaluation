package io.confluent.examples.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.parser.JSONParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.commons.codec.binary.Base64;


public class RestConsumerUtil {

    public static final String instanceString = "instance";

    public static String createConsumerGroup(String urlStr, String groupName,
           String instanceNum){
        StringBuffer buffer = new StringBuffer();
        buffer.append(urlStr);
        buffer.append(groupName);
        String response = httpPostRequest(buffer.toString(),
               buildConsumerProperty(instanceString+instanceNum));
        if (response == null || response.isEmpty()) {
            return null;
        }

        return response;
     }

    public static String buildBaseConsumerUrl(String host, int port){

        StringBuffer buf = new StringBuffer();
           buf.append("http://");
           buf.append(host);
           buf.append(":");
           buf.append(port);
           buf.append("/consumers/");
        return buf.toString();
     }

    public static JSONObject buildConsumerProperty (String instanceName) {
         JSONObject jsonFM = null;
         JSONObject toReturn = null;
         JSONArray jsonFields = new JSONArray();
         try {
             jsonFM = new JSONObject();
             //Error code 40902 – if Consumer instance with the specified name already exists.
             jsonFM.put("name",instanceName);
             jsonFM.put("format","binary");
             //jsonFM.put("format","json");
             jsonFM.put("auto.offset.reset", "smallest");
             jsonFM.put("auto.commit.enable","false");
        }

       catch (JSONException je) {
           System.out.println("Problem getting fieldMeta due to "+je);
       }
     
     return jsonFM;
     
    }

    /*Consumer Response format from Kafka
    {"instance_id":"my_consumer1","base_uri":"http://localhost:8082/consumers/testgroup/instances/my_consumer1"}
    http://localhost:8082/consumerstestgroupinstancesmy_consumer10topicstest
    GET /consumers/testgroup/instances/my_consumer/topics/test_topic HTTP/1.1
    consumerUrl + groupName + "instances" + instanceName + "topics" + topicName*/
    
    public static String buildUrlToConsume(String groupName,String instanceName, String topicName, String consumerUrl){

        StringBuffer buffer = new StringBuffer();
        buffer.append(consumerUrl);
        buffer.append(groupName);
        buffer.append("/");
        buffer.append("instances");
        buffer.append("/");
        buffer.append(instanceName);
        buffer.append("/");
        buffer.append("topics");
        buffer.append("/");
        buffer.append(topicName);

        return buffer.toString();
    }

    public static String buildBaseUrl(String host, int port){

        StringBuffer buf = new StringBuffer();
           buf.append("http://");
           buf.append(host);
           buf.append(":");
           buf.append(port);
        return buf.toString();
     }
    
    
    
    //GET /topic/test/partitions/1/messages?offset=10&count=2
    
    public static String buildUrlToConsumeInBatches (String topic, int partitionId,
            int offset, int count , String baseUrl ) {
       StringBuffer buffer = new StringBuffer();
       buffer.append(baseUrl);
       buffer.append("/topic/");
       buffer.append(topic);
       buffer.append("/partitions/");
       buffer.append(partitionId);
       buffer.append("/messages?offset=");
       buffer.append(offset);
       buffer.append("&count=");
       buffer.append(count);
       return buffer.toString();
    }
    
    public static JSONArray getDataFromKafkaInBatches ( String url) {

        String response = sendGet(url,null);
        JSONArray jsonArray = buildJsonFromString(response); 
        return jsonArray;
    }
    

    public static JSONArray getDataFromKafka(String urlStr) {
        String response = sendGet(urlStr,null);

        // The string is base64 encoded, so we have to decode it 
        // when we consume it from REST PROXY
        /* try {
            //apache common
            byte[] decoded = Base64.decodeBase64(response);
            }
            catch (UnsupportedEncodingException e) {}*/

        JSONArray jsonArray = buildJsonFromString(response);
        return jsonArray;
    }
    
    public static JSONArray buildJsonFromString (String response){
        if (response == null || response.isEmpty()) {
            return null;
        }
        JSONObject data = new JSONObject();
        JSONArray jsonarray = null;
        try {
          jsonarray = new JSONArray(response);
         
        }
        catch (JSONException jexp) {
           jexp.printStackTrace();
        }
        return jsonarray;
    }

    public static String commitOffsets (String topic, String instance,
            String host, int port) {

       String commitUrl = buildUrlToCommit(topic+"group",instance,
               buildBaseConsumerUrl(host,port));
       /* Post empty content to commit
          the offsets */
       String response = httpPostRequest(commitUrl,null);
       return response;
    }
    
    
    public static String buildUrlToCommit (String groupName,String instanceName,
               String consumerUrl) {

        StringBuffer buffer = new StringBuffer();
        buffer.append(consumerUrl);
        buffer.append(groupName);
        buffer.append("/");
        buffer.append("instances");
        buffer.append("/");
        buffer.append(instanceName);
        buffer.append("/");
        buffer.append("offsets");

        return buffer.toString();
    }
    
    public static String httpPostRequest (String url, JSONObject obj) {
        StringBuilder sb = new StringBuilder();
        try { 
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection)object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            //con.setRequestProperty("Content-Type", "application/json");
            //con.setRequestProperty("Accept", "application/json");
            // accept --> application/vnd.kafka.json.v1+json
            con.setRequestProperty("Content-Type", "application/vnd.kafka.json.v1+json");
            con.setRequestProperty("Accept", "application/vnd.kafka.v1+json, "
                    + "application/vnd.kafka+json, application/json");
            con.setRequestMethod("POST");
            
            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream());
            if (obj != null) {
                wr.write(obj.toString());
                wr.flush();
            }

            int HttpResult = con.getResponseCode(); 
            //System.out.println(HttpResult);
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
               e.printStackTrace();
               throw new IllegalArgumentException("Unable to retrieve token for MDS connection");
        }
        return sb.toString();
}
    
    
    public static String sendGet (String urlToRead, Map<String, String>headers) {
        JSONObject malformedURLException = null;
        JSONObject ioException = null;
        JSONObject protocolException = null;
        String rtnVal = null;

        try {
            malformedURLException = new JSONObject();
            ioException = new JSONObject();
            protocolException = new JSONObject();

            malformedURLException.put("Error", "Malformed URL Expcetion");
            ioException.put("Error", "IO Expcetion");
            protocolException.put("Error", "Protocaol Expcetion");

            StringBuilder result = new StringBuilder();
            URL url = new URL(urlToRead);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //conn.setRequestProperty("Content-Type", "application/vnd.kafka.json.v1+json");
            //conn.setRequestProperty("Accept", "application/vnd.kafka.v1+json, "
              //      + "application/vnd.kafka+json, application/json");
            // accept --> application/vnd.kafka.json.v1+json

            conn.setRequestMethod("GET");
            //conn.setRequestProperty("User-Agent", USER_AGENT);
            conn.setUseCaches(false);

            if (!(headers == null || headers.isEmpty())) {
                for (Map.Entry entry : headers.entrySet()) {
                    conn.addRequestProperty((String)entry.getKey(), (String)entry.getValue());
                }
            }

            // unable to get any data...
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                return null;
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            rd.close();
            rtnVal = result.toString();
        }
        catch (MalformedURLException ex) {
            rtnVal = malformedURLException.toString();
        }
        catch (IOException ex) {
            rtnVal = ioException.toString();
        }
        catch (JSONException ex) {

        }

        return rtnVal;
    }

}
