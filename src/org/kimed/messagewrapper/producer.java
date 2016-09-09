package org.kimed.messagewrapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class producer 
{
	public static void main(String[] args) {  
		
		Properties props = new Properties();
		File file = new File("config/producer.properties");
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
			return;
		}
		
		String resultTopic = props.getProperty("ResultTopic");
		
		int checkCount = 1000;
		while(checkCount-- > 0){
			try{  
	            BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
	            System.out.print("Input a path key index : (1,2,3,4...)");  
	            String str = strin.readLine();  
	            String path = "path"+str;
	            PathedMessageProducer.sendMessage(resultTopic ,path, path + " processed at nodes:");
	            //produder.flush();
			} 
			catch (IOException e){  
	            //e.printStackTrace();  
	        } 
		}
	}
}  
