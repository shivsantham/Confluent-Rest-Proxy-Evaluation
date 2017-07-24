package io.confluent.examples.producer;
import kafka.utils.ZkUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.zookeeper.ZooKeeper;
import com.google.common.collect.Lists;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import kafka.utils.ZKStringSerializer$;
import kafka.admin.RackAwareMode;
import com.google.common.collect.Lists;


public class BrokerInfoFetcher {

    public static class BrokerInfo { 
        public Integer id;
        private String host; 
        private Integer port;
		    
        public String getHost() {
            return host;
		}
		public Integer getPort() {
		    return port;
		}
		    
    }  
    
    static List<BrokerInfo> listOfBrokers (List<String> zookeeperHosts) {
    
    List<BrokerInfo> brokerList = Lists.newArrayList();
    try {
        for(String zookeeper:zookeeperHosts){
            ZooKeeper zk = new ZooKeeper(zookeeper, 10000, null);

           List<String> ids = zk.getChildren("/brokers/ids", false);
           for (String id : ids) {
                String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
                brokerList.addAll((Collection<? extends BrokerInfo>) 
                processBrokerInfoString(brokerInfo,id));
           }
        }
    }	
     catch (Exception ex) {
        ex.printStackTrace();
    }

    return brokerList;
    	
    }
	
    static List<BrokerInfo> processBrokerInfoString(String brInfoString, String brokerId) { 
        List<BrokerInfo> brInfoList = new ArrayList<BrokerInfo>(); 
        String[] info = brInfoString.split(",");
        String endPoint = null;
        String host = null;
        String port = null;
        int portNo;
        
        /*
         Sample response from the Zookeeper for the broker info
         0: {"jmx_port":-1,"timestamp":"1471735660140","endpoints":["PLAINTEXT://localhost:9092"],
         "host":"localhost","version":3,"port":9092}
         * 
         */

        if (info.length == 6) {
            host = info[3].split(":")[1].replaceAll("^.|.$", "");
            port = info[5].split(":")[1];
            portNo = Integer.parseInt(port.replaceAll("[^0-9]", ""));
            BrokerInfo brInfo = new BrokerInfo();
            brInfo.host = host.replaceAll("\\s","");
            brInfo.port = portNo;
            brInfo.id = Integer.valueOf(brokerId);
            brInfoList.add(brInfo);
        }
        
        return brInfoList;
    	
    }
    
    public static void main(String[] args) throws Exception {

	    List<String> zkList = new ArrayList<String>();
	    List<BrokerInfo> brList = new ArrayList<BrokerInfo>();
	    zkList.add("localhost:2181");
	    brList = listOfBrokers(zkList);
	    
        for(BrokerInfo br:brList) {
	           System.out.println("Broker id " + br.id + "Host" + br.host + "port number" + br.port);
	    	}
	    }
	

}
