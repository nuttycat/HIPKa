package org.kimed.messagewrapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Consumer intend to consumer {@link PathedMessage}
 * <p>Consume message of specific topic/partitions with specific {@link MessageProcessor}
 * then transpond the process result to next node according to the Path Info.(View {@link MessagePath})
 */
public class PathedMessageConsumer {
	
	private static PathedMessageConsumer sInstance = new PathedMessageConsumer();
	
	private Map<Thread, KafkaConsumer> mMapThreadToConsumer = new HashMap<Thread, KafkaConsumer>();
	
	public static PathedMessageConsumer getInstance(){
		return sInstance;
	}
	
	private PathedMessageConsumer(){
		
	}
	
	/**
	 * Register one {@link MessageProcessor} for specific topic/partitions
	 * <p>If register succeed, one Consume thread will be launched.
	 * 
	 * @return return a unique register ID if succeed, or 0 when failed.
	 */
	public static <T extends Serializable> long registerConsumeProcessor(String conumerConfigFilePath, MessageProcessor<T> processor){
		return getInstance().registerConsumeProcessorImpl(conumerConfigFilePath, processor);
	}
	
	/**
	 * Unregister specified registeration
	 * @param registerId Id of specified registeration
	 */
	public static void unregisterConsumeProcessor(long registerId){
		getInstance().unregisterConsumeProcessorImpl(registerId);
	}
	
	/**
	 * Register one {@link MessageProcessor} for specific topic/partitions
	 * <p>If register succeed, one Consume thread will be launched.
	 * 
	 * @return return a unique register ID if succeed, or 0 when failed.
	 */
	private <T extends Serializable> long registerConsumeProcessorImpl(String conumerConfigFilePath, MessageProcessor<T> processor){
		final KafkaConsumer<String, PathedMessage<T>> consumer= createKafkaConsumer(conumerConfigFilePath);
		final MessageProcessor<T> fProcessor = processor;
		Thread thread = new Thread(new Runnable(){
			public void run() {
				while (true) {
					ConsumerRecords<String, PathedMessage<T>> records = consumer.poll(100);
			         for (ConsumerRecord<String, PathedMessage<T>> record : records){
			             if(fProcessor != null){
			            	 T processResult = fProcessor.processMessage(record.value().getMessage());
			            		 
			            	 record.value().setMessage(processResult);
			            	 PathedMessageProducer.sendMessage(record.value());
			             }
			         }
			     }
			}
		});
		thread.start();
		mMapThreadToConsumer.put(thread, consumer);
		return thread.getId();	//use thread id as resister ID
	}
	
	/**
	 * Create one {@link KafkaConsumer} which intended to Consume {@link PathedMessage} 
	 * with specified configure file.
	 * @return
	 */
	private <T extends Serializable> KafkaConsumer<String, PathedMessage<T>> createKafkaConsumer(String configFilePath){
		Properties props = new Properties();
		File file = new File(configFilePath);
		try 
		{
			FileReader fReader = new FileReader(file);
			props.load(fReader);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			System.out.println("exception when load producer config!");
			e.printStackTrace();
			return null;
		}
	    
		String topicPartionStr = props.getProperty("ConsumeTopics");
		String[] topicPartions = topicPartionStr.split(",");
		TopicPartition[] partions = new TopicPartition[topicPartions.length];
		for(int i = 0; i < topicPartions.length; ++i){
			String[] sets = topicPartions[i].split("/");
			partions[i] = new TopicPartition(sets[0], Integer.decode(sets[1]));
		}
		
		KafkaConsumer<String, PathedMessage<T>> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new CommonMessageDeserializer<PathedMessage<T>>());
		consumer.assign(Arrays.asList(partions));
		return consumer;
	}
	
	private void unregisterConsumeProcessorImpl(long resigerId){
		for(Map.Entry<Thread, KafkaConsumer> entry: mMapThreadToConsumer.entrySet()){
			if(resigerId == entry.getKey().getId()){
				entry.getKey().stop();		//stop the thread
				entry.getValue().close();	//close the consumer
				mMapThreadToConsumer.remove(entry.getKey());	//remove from map
			}
		}
	}
	

}
