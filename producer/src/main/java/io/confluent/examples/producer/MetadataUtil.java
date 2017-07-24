package io.confluent.examples.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.PartitionMetadata;
import kafka.common.BrokerNotAvailableException;
import com.google.common.collect.Lists; 
import com.google.common.collect.Maps; 
import com.google.common.collect.Sets;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import kafka.utils.ZKStringSerializer$;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint; 
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import kafka.utils.ZKStringSerializer$;
import io.confluent.examples.producer.BrokerInfoFetcher.BrokerInfo;

public class MetadataUtil {

	 /*public static class BrokerInfo { 
            public Integer id;
            public String host; 
            public Integer port;
		    
            public String getHost(){
               return host;
		    }
		    public Integer getPort(){
		    	return port;
		    }
		    
		  } */ 
	
	
	 private Map<Integer, SimpleConsumer> consumerMap = Maps.newHashMap();
	 private static final String ZOOKEEPER_CONNECT = "localhost:2181";
	 private static List<String> replicaBrokers = new ArrayList<String>();

	 MetadataUtil() {
		 replicaBrokers = new ArrayList<String>();
	 }
	 
  public static List<BrokerInfo> getCluster() { 
	    
      List<Broker> brokers = getAllBrokers();
      List<BrokerInfo> brInfos = new ArrayList<BrokerInfo>();
      
      for (kafka.cluster.Broker broker : brokers) {
           BrokerInfo info = new BrokerInfo(); 
	         info.id = broker.id(); 
	         BrokerEndPoint brEndPoint = BrokerEndPoint.createBrokerEndPoint(info.id, ZOOKEEPER_CONNECT);
	         info.host = brEndPoint.host(); 
	         info.port = brEndPoint.port(); 
	         brInfos.add(info); 
      }
       
  return brInfos;
	}
	 
	public static List<Broker> getAllBrokers() {
		  
      final ZkConnection zkConnection = new ZkConnection(ZOOKEEPER_CONNECT);
	    final int sessionTimeoutMs = 10 * 1000;
	    final int connectionTimeoutMs = 20 * 1000;
	    final ZkClient zkClient = new ZkClient(ZOOKEEPER_CONNECT,
	                                  sessionTimeoutMs,
	                                  connectionTimeoutMs,
	                                  ZKStringSerializer$.MODULE$);
      final ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);

	 return scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
	}
	        
  public static List<Integer> getPartitionsForTopic(String topic, List<BrokerInfo> seedBrokers) {
		    List<Integer> partitions = new ArrayList<>();
		    for (BrokerInfo seed : seedBrokers) {
		      SimpleConsumer consumer = null;
		      try {
		        consumer = new SimpleConsumer(seed.getHost(),Integer.valueOf(seed.getPort()), 
		                   20000, 128 * 1024, "partitionLookup");
		        System.out.println("Broker -->" + seed.id + "Host name -->"+ seed.host+
		                  "Port Num -->"+seed.getPort());
		        List<String> topics = Collections.singletonList(topic);
		        TopicMetadataRequest req = new TopicMetadataRequest(topics);
		        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

		        // find our partition's metadata
		        List<TopicMetadata> metaData = resp.topicsMetadata();
		        for (TopicMetadata item : metaData) {
		          for (PartitionMetadata part : item.partitionsMetadata()) {
		            partitions.add(part.partitionId());
		          }
		        }
		        break;  // leave on first successful broker (every broker has this info)
		      } 
		      catch (Exception e) {
		        // try all available brokers, so just report error and go to next one
		        System.out.println("Error communicating with broker [" + seed + "] to find list of partitions "
		                + "for [" + topic + "]. Reason: " + e);
		      } 
		      finally {
		        if (consumer != null)
		          consumer.close();
		      }
		    }
		    return partitions;
	}
   
   
   
  /* private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
       PartitionMetadata returnMetaData = null;
       loop:
       for (String seed : a_seedBrokers) {
           SimpleConsumer consumer = null;
           try {
               consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
               List<String> topics = Collections.singletonList(a_topic);
               TopicMetadataRequest req = new TopicMetadataRequest(topics);
               kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

               List<TopicMetadata> metaData = resp.topicsMetadata();
               for (TopicMetadata item : metaData) {
                   for (PartitionMetadata part : item.partitionsMetadata()) {
                       if (part.partitionId() == a_partition) {
                           returnMetaData = part;
                           break loop;
                       }
                   }
               }
           } catch (Exception e) {
               System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                       + ", " + a_partition + "] Reason: " + e);
           } finally {
               if (consumer != null) consumer.close();
           }
       }
       if (returnMetaData != null) {
           m_replicaBrokers.clear();
           for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
               m_replicaBrokers.add(replica.host());
           }
       }
       return returnMetaData;
   }*/
   
   
   // Find the Lead Broker for a Topic Partition
   private static PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic,
                                        int partition) {
     for (String seed : seedBrokers) {
       SimpleConsumer consumer = null;
       try {
         consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
         List<String> topics = Collections.singletonList(topic);
         TopicMetadataRequest req = new TopicMetadataRequest(topics);
         kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

         List<TopicMetadata> metaData = resp.topicsMetadata();
         for (TopicMetadata item : metaData) {
           for (PartitionMetadata part : item.partitionsMetadata()) {
                if (part.partitionId() == partition) {
                    replicaBrokers.clear();
                    for (kafka.cluster.BrokerEndPoint replica : part.replicas()) {
                         replicaBrokers.add(replica.host());
                    }
                    return part;
                }
           }
         }
       } 
       catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] "
                            + " to find Leader for [" 
                            + topic + ", " + partition + "Reason: " + e);
       } 
       finally {
         if (consumer != null) 
             consumer.close();
       }
     }
     return null;
   }
   
    /* public  boolean IsActiveControllerPresent (){
	   
       ZkClient zkClient = null;
       try {
           zkClient = new ZkClient(ZOOKEEPER_CONNECT, 30000, 30000, ZKStringSerializer$.instance);
           int controllerBrokerId = ZkUtils.getController();
           String controllerInfo = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + 
                                  "/" + controllerBrokerId)._1;
           if (controllerInfo == null) {
               return false;
        	   throw new BrokerNotAvailableException("Broker id does not exist");
           }
        } catch (Throwable t) {
           //System.out.println("Operation failed due to controller failure");
           return false;
        } finally {
           if (zkClient != null)
               zkClient.close();
       }
       
       return true;
   }*/
   
  public static void main(String[] args) {
		
	     List<String> zookeeperHosts = new ArrayList<String>();
       zookeeperHosts.add("localhost:2181");
       List<Integer> out = new ArrayList<Integer>();
	     out = getPartitionsForTopic("final1",BrokerInfoFetcher.listOfBrokers(zookeeperHosts));
	}


}
