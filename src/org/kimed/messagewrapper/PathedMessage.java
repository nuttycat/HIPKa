package org.kimed.messagewrapper;

import java.io.Serializable;

public class PathedMessage<T> implements Serializable
{
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
