package org.kimed.messagewrapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class PathedMessageConsumer<T> extends Thread
{
	private KafkaConsumer<String, PathedMessage<T>> mKafkaConsumer;
	private PathedMessageProducer<T> mTranspondProducer;	
	private MessageProcessor<T> mMsgProcessor;
	private String mConsumeTopic;
	
	public PathedMessageConsumer(String configFilePath)
	{
		initConsumerFromConfig(configFilePath);
	}
	
	public void setMessageProcessor(MessageProcessor<T> processor)
	{
		mMsgProcessor = processor;
	}
	
	public void setMessageTranspondProducer(PathedMessageProducer<T> producer)
	{
		mTranspondProducer = producer;
	}
	
	public String getConsumeTopic()
	{
		return mConsumeTopic;
	}
	
	private void initConsumerFromConfig(String configFilePath)
	{
		Properties props = new Properties();
		Properties consumerProps = new Properties();
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
		}
		
		consumerProps.put("group.id", props.get("group.id"));
		consumerProps.put("bootstrap.servers", props.get("bootstrap.servers"));
		consumerProps.put("enable.auto.commit", props.get("enable.auto.commit"));
		consumerProps.put("auto.commit.interval.ms", props.get("auto.commit.interval.ms"));
		consumerProps.put("session.timeout.ms", props.get("session.timeout.ms"));
		
		String[] contopics = props.get("consumetopics").toString().split(",");
	    Collection<String> consumeTopics = new ArrayList<String>();
	    for(int i = 0; i < contopics.length; ++i)
	    	consumeTopics.add(contopics[i]);
	    mConsumeTopic = contopics[0];
	    
	    mKafkaConsumer = new KafkaConsumer<>(props, new StringDeserializer(), new CommonMessageDeserializer<PathedMessage<T>>());
	    mKafkaConsumer.subscribe(consumeTopics);
	}
	
	public void run()
	{
	    while (true) 
	     {
	         ConsumerRecords<String, PathedMessage<T>> records = mKafkaConsumer.poll(100);
	         for (ConsumerRecord<String, PathedMessage<T>> record : records)
	         {
	             if(mMsgProcessor != null)
	             {
	            	 T processResult = mMsgProcessor.processMessage(record.value().getMessage());
	            	 if(mTranspondProducer != null)
	            	 {
	            		 record.value().setMessage(processResult);	//���������·����Ϣ
	            		 mTranspondProducer.sendMessage(record.value());	//ת��
	            	 }
	             }
	         }
	     }
	}
}
