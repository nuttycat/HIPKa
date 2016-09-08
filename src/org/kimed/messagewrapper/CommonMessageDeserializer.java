package org.kimed.messagewrapper;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class CommonMessageDeserializer<T> implements Deserializer<T>
{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public T deserialize(String topic, byte[] data) 
	{
		ByteArrayInputStream byteStream;
		ObjectInputStream objStream;
		T result;
		try 
		{
			byteStream = new ByteArrayInputStream(data);
			objStream = new ObjectInputStream(byteStream);
			result = (T)objStream.readObject();
		} 
		catch (Exception e) 
		{
			System.out.println("exception when deserialize message");
			e.printStackTrace();
			return null;
		}
		return result;
	}

}
