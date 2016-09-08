package org.kimed.messagewrapper;

import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class CommonMessageSerializer<T> implements Serializer<T>
{

	@Override
	public void close() 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) 
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String topic, T msg) 
	{
		ByteArrayOutputStream byteStream;
		ObjectOutputStream objStream;
		byte[] result;
		try 
		{
			byteStream = new ByteArrayOutputStream();
			objStream = new ObjectOutputStream(byteStream);
			objStream.writeObject(msg);
			objStream.close();
			result = byteStream.toByteArray();
			byteStream.close();
		} catch (IOException e) 
		{
			System.out.println("exception when serialize PathedMessage.");
			e.printStackTrace();
			return null;
		}
		return result;
	}

}
