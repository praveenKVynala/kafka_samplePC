package com.citi;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaConsumer1 {
	
	
    final static List<String> TOPICS = Arrays.asList("topic");
    final static Properties props = new Properties();
	
    public static void main(String[] args){

    	//properties for Consumer
    	props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092" );
    	props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test-group1" );
    	props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongDeserializer" );
    	props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer" );
    	props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
    	
    	
    	
    	
    	
        Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);
        consumer.subscribe(TOPICS);
        
        try{        
        	while (true) {
        		
	            ConsumerRecords<Long, String> consumerRecords =consumer.poll(Duration.ofMillis(100));
	            consumerRecords.forEach(record -> 
	            	{
	            	  System.out.printf("\nKey\t:%d",record.key());
		              System.out.printf("\nValue\t:%s", record.value());
		              System.out.printf("\nTopic Name\t:%s",record.topic());
		              System.out.printf("\nPartition Name\t:%d", record.partition());
		              System.out.printf("\nOffset Value\t:%d",record.offset());
	            });
	            consumer.commitAsync();
        	}
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        finally {
            consumer.close();
            System.out.println("Done");
        }
	}
}	
	
