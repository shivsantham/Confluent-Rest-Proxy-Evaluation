package io.confluent.examples.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;



public class KafkaHelper {


	public static void seekToBeginning(final KafkaConsumer<?, ?> consumer, final String topic) {
	        synchronized (consumer) {
	            Set<String> subscription = consumer.subscription();
	            try {
	                consumer.unsubscribe();
	                List<PartitionInfo> partInfo = consumer.partitionsFor(topic);
	                for (PartitionInfo p : partInfo) {
	                    TopicPartition tp = new TopicPartition(topic, p.partition());
	                    consumer.assign(Arrays.asList(tp));
	                    consumer.seekToBeginning(tp);
	                    consumer.position(tp);
	                    consumer.commitSync();
	                }
	            } finally {
	                consumer.unsubscribe();
	                List<String> topics = new ArrayList<String>();
	                topics.addAll(subscription);
	                consumer.subscribe(topics);
	            }
	        }
	}


	public static void seekToEnd(final KafkaConsumer<?, ?> consumer, final String topic) {
	        synchronized (consumer) {
	            Set<String> subscription = consumer.subscription();
	            try {
	                consumer.unsubscribe();
	                List<PartitionInfo> partInfo = consumer.partitionsFor(topic);
	                for (PartitionInfo p : partInfo) {
	                    TopicPartition tp = new TopicPartition(topic, p.partition());
	                    consumer.assign(Arrays.asList(tp));
	                    consumer.seekToEnd(tp);
	                    consumer.position(tp);
	                    consumer.commitSync();
	                }
	            } finally {
	                consumer.unsubscribe();
	                List<String> topics = new ArrayList<String>();
	                topics.addAll(subscription);
	                consumer.subscribe(topics);
	            }
	        }
	}

}
