 package io.confluent.examples.producer;

import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;



public class RestMonitoringUtil {

/*
	//Get a list of Kafka topics.
	private static String topicUrl = "http://localhost:8082/topics/";
	private static String brokerUrl = "http://localhost:8082/brokers";
	
	
	
	public static String getTopicsFromCluster(String topicUrl){
		JSONArray topics = RestConsumer.getDataFromKafka(topicUrl);
		return topics.toString();
	}
	
	public static String getBrokersFromCluster(String brokerUrl){
		JSONObject brokers = RestConsumer.getFromKafka(brokerUrl);
		return brokers.toString();
	}
	
	
	/*HTTP/1.1 200 OK
	Content-Type: application/vnd.kafka.v1+json

	{
	  "name": "test",
	  "configs": {
	     "cleanup.policy": "compact"
	  },
	  "partitions": [
	    {
	      "partition": 1,
	      "leader": 1,
	      "replicas": [
	        {
	          "broker": 1,
	          "leader": true,
	          "in_sync": true,
	        },
	        {
	          "broker": 2,
	          "leader": false,
	          "in_sync": true,
	        }
	      ]
	    },
	    {
	      "partition": 2,
	      "leader": 2,
	      "replicas": [
	        {
	          "broker": 1,
	          "leader": false,
	          "in_sync": true,
	        },
	        {
	          "broker": 2,
	          "leader": true,
	          "in_sync": true,
	        }
	      ]
	    }
	  ]
	}*/
	
	/*
	public static Integer getLeadBrokerForTopicPartition(String topic,int partition){
       int id=0;
       JSONObject response = RestConsumer.getFromKafka(new StringBuffer(topicUrl).
                         append(topic).toString());
       
       System.out.println("This is the response"+ response.toString());
       try {
           JSONArray partitions = response.getJSONArray("partitions");
    	   for (int i = 0; i < partitions.length(); ++i) {
    		   JSONObject part = partitions.getJSONObject(i);
    		   if(partition == i){
    			   id = part.getInt("leader");
    			   break;
    		   }
    	   }
        
       }
       catch (JSONException je) {
           System.out.println("Problem getting leader due to "+je);
       }
       return id;
    }
	
	
    	
	
	
	public static void main(String[] args) {
	
      // System.out.println(getTopicsFromCluster(topicUrl));
       System.out.println(getLeadBrokerForTopicPartition("final1",1));
       System.out.println(getBrokersFromCluster(brokerUrl));
	}

*/

}

