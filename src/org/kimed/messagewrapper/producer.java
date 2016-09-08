package org.kimed.messagewrapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class producer 
{
	public static void main(String[] args) {  
		PathedMessageProducer<String> produder = new PathedMessageProducer<String>("config/producer.properties");
		int checkCount = 1000;
		while(checkCount-- > 0){
			try{  
	            BufferedReader strin=new BufferedReader(new InputStreamReader(System.in));  
	            System.out.print("");  
	            String str = strin.readLine();  
	            String path = "path"+str;
	            produder.sendMessage(path, path + " processed at nodes:");
	            //produder.flush();
			} 
			catch (IOException e){  
	            //e.printStackTrace();  
	        } 
		}
		produder.close();
	}
}  
