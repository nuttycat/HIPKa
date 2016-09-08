package org.kimed.messagewrapper;

public interface MessageProcessor<T>{
	public T processMessage(T srcMsg);
}


class TestMessageProcessor implements MessageProcessor<String>{

	private String mProcessorName;
	@Override
	public String processMessage(String srcMsg){
		String result = srcMsg + mProcessorName + " ";
		System.out.println(result);
		return result;
	}
	
	public void setmProcessorName(String name){
		mProcessorName = name;
	}
}