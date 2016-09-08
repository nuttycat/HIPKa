package org.kimed.messagewrapper;

public class Consumer
{
	public static void main(String[] args)
	{
		PathedMessageProducer<String> produder = new PathedMessageProducer<String>("config/producer.properties");
		
		TestMessageProcessor processor = new TestMessageProcessor();
		PathedMessageConsumer<String> consumer = new PathedMessageConsumer<String>("config/consumer.properties");
		processor.setmProcessorName(consumer.getConsumeTopic());
		consumer.setMessageTranspondProducer(produder);
		consumer.setMessageProcessor(processor);
		consumer.start();
	}
}
