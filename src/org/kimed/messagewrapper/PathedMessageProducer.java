package org.kimed.messagewrapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.*; 
import org.apache.kafka.common.serialization.StringSerializer;

public class PathedMessageProducer<T>
{
	private Producer<String, PathedMessage<T>> mKafkaProducer;
	private MessagePathFactory	mMsgPathFactory;
	private String mResponseTopic;
	
	public PathedMessageProducer(String configFilePath)
	{
		mMsgPathFactory = MessagePathFactory.getInstance();
		initProducerFromConfig(configFilePath);
	}
	

	public boolean sendMessage(String pathKey, T message)
	{
		PathedMessage<T> msg = new PathedMessage<T>();
		
		MessagePath path = mMsgPathFactory.createMessagePathByKey(pathKey);
		
		if(null != path)
		{
			path.appendPathNode(mResponseTopic);
			msg.setPath(path);
			msg.setMessage(message);
			
			sendMessage(msg);
			return true;
		}
		else
			return false;
	}
	
	public boolean sendMessage(PathedMessage<T> pathedMessage)
	{
		String nextTopic = pathedMessage.nextNode();
		if(nextTopic != null && nextTopic.isEmpty() != true)
		{
			mKafkaProducer.send(new ProducerRecord<String, PathedMessage<T>>(nextTopic, "1", pathedMessage));
		}
		return true;
	}
	
	public void close()
	{
		mKafkaProducer.close();
	}
	
	public void flush()
	{
		mKafkaProducer.flush();
	}
	
	private void initProducerFromConfig(String configFilePath)
	{
		Properties props = new Properties();
		Properties producerProps = new Properties();
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
		
		producerProps.put("bootstrap.servers", props.get("bootstrap.servers"));
		producerProps.put("acks", props.get("acks"));
		producerProps.put("retries", props.get("retries"));
		producerProps.put("batch.size", props.get("batch.size"));
		producerProps.put("linger.ms", props.get("linger.ms"));
		producerProps.put("buffer.memory", props.get("buffer.memory"));
		
		mResponseTopic = props.get("bindtopic").toString();
		
		mKafkaProducer = new KafkaProducer<>(props, new StringSerializer(), new CommonMessageSerializer<PathedMessage<T>>());	
	}
	
}
