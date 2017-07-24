package io.confluent.examples.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.json.JSONException;

public class RestProducerGroup {

    private int noOfThreads;
    private String[] topicList;
    private ExecutorService executor;
    private String host;
    private int port;
    private JSONObject dataJson;
    private int noOfMessages;
    private static int PARTITION_FACTOR = 10;
    static Object lock = new Object();
    public static long totalTimeProducing;

    RestProducerGroup (int noThreads, String[] topicList,String host, 
                      int port,JSONObject dataJson, int numMessages) {
        noOfThreads = noThreads;
        this.topicList = topicList;
        this.host = host;
        this.port = port;
        this.dataJson = dataJson;
        noOfMessages = numMessages;
    }

    public void run(int numThreads) {

        System.out.println("Spawning threads");
        executor = Executors.newFixedThreadPool(numThreads);

        int index;
        int count = 0;
        
        for (int i=0; i< numThreads; i++) {
            index = i%PARTITION_FACTOR;
            if(index==0)
               count++;
               
            executor.submit(new RestProducerThread(
                    topicList[count-1],host,port,JSONUtil.buildJsonObject(
                    topicList[count-1],topicList,dataJson),noOfMessages)
                    );
        }

    }


    public void shutDown() {
        
        if (executor != null) 
            executor.shutdown();
        
        try {
 
              if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                   System.out.println("Timed out on creating producers "
                         + " threads to shut down "
                         + " exiting uncleanly ");
               }
        } 
        
        catch (InterruptedException e) {
               System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }
    
 
    public static void main(String[] args) {

        int noThreadsPerTopic = 10;
        String host = "localhost";
        String topics = args[0];
        int numOfMessages = Integer.valueOf(args[1]); 
        String[] topicArray = topics.split(",");
        int noOfThreads = topicArray.length*noThreadsPerTopic;
        int port = 8082;

        ArrayList<String> topicList = new ArrayList<String>(
                                      Arrays.asList(topicArray));

        /*
         * Create the topics and partitions before hand
         * using the native API, this is not supported yet 
         * in Confluent 3.0.0, this is supposed to be released
         * in the later versions.
         */
        ZookeeperUtil.createTopics(topicList, 5, 3);
        JSONObject json = null;
        try {
             json = new JSONObject("{'var0':'AAAAA','var1':'BBBBB','var2':'CCCCC',"
                    + "'var3':'DDDDD','var4':'EEEEE'}");
        }
        catch (JSONException e) {
        }

        RestProducerGroup rpg = new RestProducerGroup(noOfThreads,topicArray,
                                    host,port,json,numOfMessages);

        Long startTime = System.currentTimeMillis();
        System.out.println("Spawning " + noOfThreads + "REST Producer Threads");
        
        
        rpg.run(noOfThreads);
        
        for (int i=0; i<topicArray.length;i++) {
            long noProcessed = 0;
            while (noProcessed < numOfMessages) {
                     synchronized(lock) {
                        noProcessed = RestProducerThread.getTopicCount
                                (topicArray[i]);
                     }
                   /* if (noOfMessages > 10000) {
                          try {
                             Thread.sleep(60000);
                          } catch (InterruptedException ie) {
                            }
                      }
                      else {
                         try { 
                            Thread.sleep(10);
                          } catch (InterruptedException ie) {}
                      }*/
              }
         }
        
        Long endTime = System.currentTimeMillis();
        double totalTime = (endTime - startTime);
        double totalTimeInSecs = ((double) totalTime)/1000;
        
        long noOfMessagesSent = noOfThreads * numOfMessages;
        System.out.println("Total Messages :" + noOfMessagesSent );
        System.out.println( " No of Messages produced per second ::" + 
                 ((double)noOfMessagesSent)/totalTimeInSecs);

        rpg.shutDown();

    }
}
