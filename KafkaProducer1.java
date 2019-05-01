
package com.citi;


import java.util.Properties;
import java.io.*;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;



public class KafkaProducer1 {

	
	final static Properties props = new Properties();
	final static String TOPIC_NAME = "topic";
  	       
	public static void main(String[] args) throws Exception {
		
        //properties for producer
		
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
//		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//		props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
//		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
//		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//		props.setProperty(ProducerConfig.RETRIES_CONFIG,"3");
//		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10");
//		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		
        
        Producer<Long, String> producer = new KafkaProducer<Long, String>(props);
        BufferedReader br = null;
  	  Long key=0l;
  	  String value;
        
        try {
	    	  File file = new File("/usr/local/confluent-5.1.2/logs/server.log"); 	    	  
	    	  br = new BufferedReader(new FileReader(file)); 

	    	  
	    	  while (true) { 
	    		  value = br.readLine();
	              if (value!= null){
		            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME,key,"["+value+"]" );
		            producer.send(record, new Callback() {
						@Override
						public void onCompletion(RecordMetadata recordMetaData, Exception e) {
							if (e== null) {
					           	System.out.printf("\n Messag Sent Successfully to Kafka: \n");
					           	System.out.printf("\n Topic Name \t: %s",recordMetaData.topic());
					           	System.out.printf("\n Partition Number \t: %d", recordMetaData.partition());
					           	System.out.printf("\n Offset \t: %d",recordMetaData.offset());
					           	System.out.printf("\n TimeStamp \t: %s", recordMetaData.timestamp());
					           								 
							}
							else {
								System.err.printf("Error while sending Message :%s \n",e);
							}
						}
					});
		            producer.flush();
//		            System.out.print(producer.);
		            key++;

	              }
	    	  	}
	          }	      	 
	      finally {
	          producer.close();
	          br.close();
	          System.out.println("Producer Stopped");
	      }
        
        
        
		}
	 
	
}	
	



