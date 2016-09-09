package org.kimed.messagewrapper;

public class Consumer
{
	public static void main(String[] args)
	{
		TestMessageProcessor processor = new TestMessageProcessor();
		
		String conumerNodeName;
		if(args.length > 0){
			conumerNodeName = args[0];
		}
		else{
			conumerNodeName = "ConsumeNode" + System.currentTimeMillis();
		}
		
		System.out.println(conumerNodeName);
		
		processor.setProcessorName(conumerNodeName);
		
		PathedMessageConsumer.registerConsumeProcessor("config/consumer.properties", processor);
	}
}
