package org.kimed.messagewrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MessagePath implements Serializable, Cloneable {
	private List<String> mPath= new ArrayList<>();
	int position=0;
	static final long serialVersionUID = 10000L;
	//
	public MessagePath(String[] nodes){
		for(int i = 0; i < nodes.length; ++i)
			mPath.add(nodes[i]);
	}
	
	public void appendPathNode(String node){
		mPath.add(node);
	}
	
	public String nextNode(){
		if(position < mPath.size() - 1){
			String nextnode = mPath.get(position);
			++position;
			return nextnode;
		}
		else{
			return null;	
		}
	}
	
	public MessagePath clone() {
		MessagePath o;
		try 
		{
			o = (MessagePath)super.clone();
			o.mPath = new ArrayList<>(mPath);
			o.position = position;
		} 
		catch (CloneNotSupportedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return o;
	}
}
