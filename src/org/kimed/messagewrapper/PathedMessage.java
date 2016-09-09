package org.kimed.messagewrapper;

import java.io.Serializable;

/**
 * Message with route path info, node of path is kafka topic.
 *
 * @param <T> origin message prototype
 */
public class PathedMessage<T extends Serializable> implements Serializable
{
	/** serial version, should ensure same in serialize and deserialize port*/
	static final long serialVersionUID = 10000L;
	
	private MessagePath mMsgPath;
	private T mMsg;
	
	
	public void setPath(MessagePath path)
	{
		mMsgPath = path;
	}
	
	public String nextNode()
	{
		return mMsgPath.nextNode();
	}
	
	public T getMessage()
	{
		return mMsg;
	}
	
	public void setMessage(T msg)
	{
		mMsg = msg;
	}
}
