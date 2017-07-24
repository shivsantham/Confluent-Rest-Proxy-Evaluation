package io.confluent.examples.producer;
import java.io.FileReader;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.parser.JSONParser;

public class JSONUtil {

    public static JSONObject retrieveData(String filepath) {
        
        JSONObject obj = null;
        try {
           FileReader reader = new FileReader(filepath);
           JSONTokener tokener = new JSONTokener(reader);
           obj = new JSONObject(tokener);
           System.out.println(obj.toString());
        } 
        catch (Exception e) {
               e.printStackTrace();
        }
        return obj;
    }

    @SuppressWarnings("unchecked")
          /*
          Host: kafkaproxy.example.com
          Content-Type: application/vnd.kafka.binary.v1+json
          Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json
          {
           "records": [
            {
             "key": "somekey",
             "value": {"foo": "bar"}
            },
           {
            "value": [ "foo", "bar" ],
             "partition": 1
           },
           {
           "value": 53.5
           }]
          }*/

    public static JSONObject buildJsonObject (String topic,String[] topicList, 
                                              JSONObject json) {
        JSONObject parentObj = new JSONObject();
        try {
              // Create a bunch of json objects 
              // And add it into JSON array to avoid
              // HTTP round trip
              JSONObject obj = new JSONObject(); 
              obj.put("key", "somekey");
              obj.put("value", generate10kCharacters(getData(topic,topicList,json)));
              JSONArray jsonArray = new JSONArray();
              jsonArray.put(obj);
              parentObj.put("records", jsonArray);
        }
        catch (JSONException e){
        }
        
        return parentObj;
    }

    public static String getData(String topicName, String[] topicList, JSONObject json){
           String toReturn = null;
           for (int i=0; i<topicList.length;i++) {
                if (topicName != null && topicName.equals(topicList[i])) {
                    try {
                      toReturn = json.getString("var"+String.valueOf(i));
                    }
                    catch (JSONException e) {
                    }
                }
           }

      return toReturn;
    }

    public static StringBuffer generate10kCharacters(String s) {
        StringBuffer buffer = new StringBuffer();
        for(int i = 0 ; i< 200;i++){
            buffer.append(s);
        }
      return buffer;
    }

    public static String retrieveData(String topicName, String filepath,
                                      String[] topicList) {
          String toReturn = null;
          JSONParser parser = new JSONParser();
          JSONObject jsonObject = null;
          try {
            Object obj = parser.parse(new FileReader(filepath));
            jsonObject = (JSONObject) obj;
          } 
          catch (Exception e) {
                e.printStackTrace();
	        }

      return null;
    }

   public static void main (String[] args) {

        String path = "/generated.json";
        System.out.println(retrieveData(path).toString());
   }
}
