package io.confluent.examples.consumer;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.sql.Timestamp;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Date;
import org.json.JSONArray;

public class RestConsumerThread implements Runnable {

    private String topic;
    private String host;
    private int port;
    private int threadNumber;
    private String url;
    private String group;
    private CyclicBarrier barrier;
    static Object lock = new Object();
    private long startTime;
    private long elapsedTime;
    private long noMessages;
    private boolean BATCH_MODE;
    Date date;


    RestConsumerThread(String topic,String host,int port,
              int threadNumber,String group, CyclicBarrier barrier, int batchMode) {
        this.topic = topic;
        this.host = host; 
        this.port = port;
        this.threadNumber = threadNumber;
        this.group = group;
        this.barrier = barrier;
        date= new java.util.Date();
        BATCH_MODE = (batchMode == 1) ? true:false; 
        init();
    }

    public void init() {
        String baseUrlToHit = RestConsumerUtil.buildBaseConsumerUrl(host,port); // http://localhost:8082/consumers
        //http://localhost:8082/consumers/TopicGroupName/ 
        //-- Post this for every topic with an instance name
       // System.out.println("The base url we are trying to hit -->"
         //     + baseUrlToHit);

        /*
         * POST /consumers/TopicgroupName/ HTTP/1.1
           Host: kafkaproxy.example.com
              Accept: application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json
            {	
            "name": "my_consumer", --> instance name 
            "format": "binary",
            "auto.offset.reset": "smallest",
            "auto.commit.enable": "false"
            }*/

        /* Response for above
         * HTTP/1.1 200 OK
           Content-Type: application/vnd.kafka.v1+json
           {
           "instance_id": "my_consumer",
           "base_uri": "http://proxy-instance.kafkaproxy.example.com/consumers/testgroup/instances/my_consumer"
           }
         */

        
        // Bind the threads to the consumer group instance.
        String response = RestConsumerUtil.createConsumerGroup(baseUrlToHit,
                    group,String.valueOf(threadNumber)); 
        url = RestConsumerUtil.buildUrlToConsume(group,RestConsumerUtil.instanceString+
              String.valueOf(threadNumber),topic,baseUrlToHit);
        
    }

    public void run() {
         
       /* Right now the messages received in the consumer side
        * is all base64 encoded, making a GET request with the JSON 
        * type doesnt seem to solve the problem.. May need to decode 
        * manually to fix
       */
        while (true) {
              try {
                 startTime = System.currentTimeMillis();
                 //Thread.sleep(1000);
                 /*
                  * For consuming in batches
                  * Parse the last element in the JSON Array
                  * To read the offset info. Store it in a 
                  * Map <Topic,Offset> when the consumer is Open all the time
                  * When getting messages from, use this offset to start
                  * consuming the messages from to poll in batches.
                  * 
                  * 
                  *   getDataFromKafkaInBatches(url);
                  */ 
                 
                 
                  synchronized (lock) {
                      
                      /*if (BATCH_MODE) {
                          // this is for first call
                          url = RestConsumerUtil.buildUrlToConsumeInBatches(topic, 0, 0, 100,
                                RestConsumerUtil.buildBaseUrl(host, port));
                          
                          JSONArray batchedMessage = RestConsumerUtil.getDataFromKafkaInBatches(url);
                          noMessages = batchedMessage.length();
                          JSONObject lastMessageOffsetInfo = batchedMessage.getJSONObject(noMessages-1);
                          int partition = lastMessageOffsetInfo.getInt("parition");
                          int offset = lastMessageOffsetInfo.getInt("offset");
                          
                          // read JSONArray
                      }*/
                      //else
                      int partition=0;
                      JSONArray kafkaMessage = RestConsumerUtil.getDataFromKafka(url);
                      noMessages = kafkaMessage.length();
                      RestConsumerGroup.totalMessagesConsumed += noMessages;
                      if (noMessages > 1) {
                          partition = kafkaMessage.getJSONObject(0).getInt("partition");
                      }
                      /* Commit all offsets on specific topic & instance
                       * Note: Commit request must be made to the specific REST 
                       * proxy instance holding the consumer instance.
                       */
                      
                      String instanceId = RestConsumerUtil.instanceString + 
                              String.valueOf(threadNumber);
                      String response = RestConsumerUtil.commitOffsets(topic,
                            instanceId,host,port);
                      
                      long endTime = System.currentTimeMillis();
                      elapsedTime =  endTime - startTime ;
                      RestConsumerGroup.totalTimeConsuming += elapsedTime;
                      
                      if(noMessages > 0) {
                             System.out.println(new Timestamp(date.getTime()) +
                             " :] Thread :" + threadNumber + " Consumer Group: " +instanceId+ 
                             " took " + ((double) elapsedTime)/1000  + "  seconds" +
                             " to consume " + noMessages + " messages on topic " + topic +
                             " partition: " + partition );
                      }

                  }
              }
              catch (Exception ex) {
                     ex.printStackTrace();
              }
              // this should be done after every second
              try  {
                 barrier.await();
                 synchronized (lock) {
                    RestConsumerGroup.startTime = System.currentTimeMillis();
                  }
              }
              catch (InterruptedException e) {
                     e.printStackTrace();
              }
              catch (BrokenBarrierException e) {
                     e.printStackTrace();
              }
       }
   }

}
