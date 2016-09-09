package org.kimed.messagewrapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Producer intend to send {@link PathedMessage}
 */
public class PathedMessageProducer {
	
	/** To cache created kafka producer(intend to send {@link PathedMessage}), keyed by message prototype calss*/
	private HashMap<Class, Producer> mProducers = new HashMap<>();
	
	
	private static PathedMessageProducer sInstance = new PathedMessageProducer();
	private PathedMessageProducer() {
		
	}
	public static PathedMessageProducer getInstance(){
		return sInstance;
	}
	
	/**
	 * Send the message with a message route info({@link MessagePath})
	 * @param <T> message prototype
	 * @param resultTopic the last node(in fact is a kafka topic) which the message should finally be sent to
	 * @param pathKey the key which used to get preloaded {@link MessagePath} from {@link MessagePathFactory}
	 * @param message data to be sent
	 * @return future data if send successful , or null
	 */
	public static <T extends Serializable> Future<RecordMetadata> sendMessage(String resultTopic, String pathKey, T message) {
		PathedMessage<T> msg = new PathedMessage<T>();
		MessagePath path = MessagePathFactory.getInstance().createMessagePathByKey(pathKey);
		
		if(null != path)
		{
			path.appendPathNode(resultTopic);
			msg.setPath(path);
			msg.setMessage(message);
			
			return sendMessage(msg);
		}
		else
			return null;
	}
	
	/**
	 * Send {@link pathedMessage}
	 * @param <T> prototype of specific message
	 * @return future data if send successful , or null
	 */
	public static <T extends Serializable> Future<RecordMetadata> sendMessage(PathedMessage<T> pathedMessage) {
		return sInstance.sendMessageImpl(pathedMessage);
	}
	
	/**implementation of SendMessage*/
	private <T extends Serializable> Future<RecordMetadata> sendMessageImpl(PathedMessage<T> pathedMessage) {
		String nextTopic = pathedMessage.nextNode();
		if(nextTopic != null && nextTopic.isEmpty() != true)
		{
			//get cached kafka producer
			Producer<String, PathedMessage<T>> producer = getKafkaProducer((Class<T>)pathedMessage.getMessage().getClass());
			System.out.println("produce to " + nextTopic);
			return producer.send(new ProducerRecord<String, PathedMessage<T>>(nextTopic, "1", pathedMessage));
		}
		return null;
	}
	
	/**
	 * Try to get the saved instance of kafka producer instance of 
	 * specified message prototype T from cache map,if get null, create 
	 * a new one, and cached into map
	 * @param <T> message prototype
	 * @param c class instance of message prototype
	 */
	private <T extends Serializable> Producer<String, PathedMessage<T>> getKafkaProducer(Class<T> c){
		Producer<String, PathedMessage<T>> producer = mProducers.get(c);
		if(producer == null)
		{
			producer = createKafkaProducer("config/KafkaProducer.properties");
			mProducers.put(c, producer);
		}
		return producer;
	}
	
	/**
	 * create one producer with configuration file
	 * @param <T> message prototype
	 * @param configFilePath producer configure file path
	 * @return : created instance of kafka producer intend to producer pathed message
	 */
	private <T extends Serializable> Producer<String, PathedMessage<T>> createKafkaProducer(String configFilePath){
		Producer<String, PathedMessage<T>> producer;
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
		
		//mResponseTopic = props.get("bindtopic").toString();
		producer = new KafkaProducer<>(props, new StringSerializer(), new CommonMessageSerializer<PathedMessage<T>>());
		return producer;
	}
	
	/**
	 * Flush kafka producer for specified Message prototype
	 * @param c class instance of message prototype
	 */
	public <T extends Serializable> void flush(Class<T> c){
		Producer<String, PathedMessage<T>> producer = mProducers.get(c);
		if(null != producer){
			producer.flush();
		}
	}
	
	/**
	 * Flush all cached kafka producer
	 */
	public void flush(){
		for(Producer p : mProducers.values()){
			p.flush();
		}
	}
	
	
	/**
	 * Close all kafka producer instance
	 */
	public void close(){
		for(Producer p : mProducers.values()){
			p.close();
		}
		mProducers.clear();	//all producer closed, clear them
	}
}
