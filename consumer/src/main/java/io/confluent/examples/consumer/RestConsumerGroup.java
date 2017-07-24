package io.confluent.examples.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import io.confluent.examples.consumer.RestConsumerThread;

public class RestConsumerGroup {

    private int noOfThreads;
    private String topic;
    private ExecutorService executor;
    private String host;
    private int port;
    private String groupName;
    private static String groupString = "group";
    public static long startTime;
    public static long endTime;
    public static double totalTime;
    public static long totalTimeConsuming = 0 ; 
    public static long totalMessagesConsumed = 0;
    public static int barriercount = 0;
    static Object lock = new Object();
    private int batchMode;

    RestConsumerGroup(int noThreads, String topic,String host, 
            int port, int batchMode) {
        noOfThreads = noThreads;
        this.topic = topic;
        this.host = host;
        this.port = port;
        this.batchMode = batchMode;
    }

    public void run(int numThreads,CyclicBarrier cb) {

        executor = Executors.newFixedThreadPool(numThreads);
        
        for (int i=0; i< numThreads; i++) {
             executor.submit(new RestConsumerThread(topic,host,port,i,
                  topic+groupString,cb,batchMode));
        }

    }

    public void shutDown() {
        if (executor != null) 
            executor.shutdown();
        
        try {
 
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                   System.out.println(
                    "Timed out on creating prodcuers threads to shut down,"
                   + " exiting uncleanly");
            }
        }
        
        catch (InterruptedException e) {
               System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }
	
    public static CyclicBarrier collectMetrics (int noOfThreads) {

        startTime = System.currentTimeMillis();
        CyclicBarrier cb = new CyclicBarrier(noOfThreads, new Runnable() {
            @Override
            public void run() {
                //This task will be executed once 
               //all threads reache barrier
                
                if (barriercount == Integer.MAX_VALUE) {
                    return;
                }
                barriercount++;
                endTime = System.currentTimeMillis();
                totalTime = endTime - startTime;
                double totalTimeInSecs = ((double) totalTimeConsuming)/1000;

                if (totalMessagesConsumed > 0) {
                    System.out.println("Total Number of Messages" + totalMessagesConsumed ); 
                    System.out.println( " Consumption Throughput :: " + ((double)totalMessagesConsumed)/totalTimeInSecs + 
                                        "Messages per second" );
                }
                else {
                     System.out.println("No metrics to collect");
                }

                synchronized (lock) {
                    totalMessagesConsumed = 0;
                    totalTimeConsuming = 0;
                }
                
            }
        });

        return cb;
   }

    public static void main(String[] args) {

        int noThreadsPerTopic = 10;
        String host = "localhost";
        String topics = args[0];
        int batchMode = Integer.valueOf(args[1]);
        String[] topicList = topics.split(",");
        int noOfThreads = topicList.length*noThreadsPerTopic;
        int port = 8082;

        List<RestConsumerGroup> rcList = new ArrayList<RestConsumerGroup>();
        RestConsumerGroup rc = null;

        CyclicBarrier metricsBarrier = collectMetrics(noOfThreads);
        // Launching consumergroups -- with 10 threads in a group 
        for (int i=0; i<topicList.length;i++) {
             rc = new RestConsumerGroup(noThreadsPerTopic,topicList[i],
                  host,port,batchMode);
             rcList.add (rc);
             rc.run(noThreadsPerTopic,metricsBarrier);
        }

 

   }



}
