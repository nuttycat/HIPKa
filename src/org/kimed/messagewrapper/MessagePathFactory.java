package org.kimed.messagewrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class MessagePathFactory 
{
	private Map<String, MessagePath> mMapPathKeyToPath = new HashMap<String, MessagePath>();
	
	private static MessagePathFactory sInstance = new MessagePathFactory();
	private MessagePathFactory()
	{
		loadMessagePaths("config/messagepath.cfg");
	}
	
	public static MessagePathFactory getInstance()
	{
		return sInstance;
	}
	
	
	public MessagePath createMessagePathByKey(String msgPathKey)
	{
		MessagePath path = mMapPathKeyToPath.get(msgPathKey);
		if(null != path)
			return path.clone();
		else
			return null;
	}
	
	private void loadMessagePaths(String pathConfigFilePath){
		//mMapPathKeyToPath;
		File file = new File(pathConfigFilePath);
        BufferedReader reader = null;
        try 
        {
        	System.out.println("");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) 
            {
            	String[] pathInfo = tempString.split(":");
            	String[] pathNodes = pathInfo[1].split(",");
            	String pathKey = pathInfo[0];
            	mMapPathKeyToPath.put(pathKey, new MessagePath(pathNodes));
            }
            reader.close();
        } catch (Exception e) 
        {
            e.printStackTrace();
        }
	}

}
