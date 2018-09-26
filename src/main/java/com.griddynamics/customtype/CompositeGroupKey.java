package com.griddynamics.customtype;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class to hold username and sessionId as a grouped key. Implements WritableComparable for reading, writing and comparing
 * objects.
 */
public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
	private String username;
	private String sessionId;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public CompositeGroupKey(){
		this.username=null;
		this.sessionId=null;
	}

	public CompositeGroupKey(String uname,String sId){
		this.username=uname;
		this.sessionId=sId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, username);
		WritableUtils.writeString(out, sessionId);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.username = WritableUtils.readString(in);
		this.sessionId = WritableUtils.readString(in);
	}

	@Override
	public int compareTo(CompositeGroupKey that) {
		int result=0;
		if (that == null)
			return 0;
		int userComp = that.username.toLowerCase().compareTo(username);
		if(userComp!=0) result= userComp;
		else{
			int sessionComp = that.sessionId.toLowerCase().compareTo(sessionId);
			if(sessionComp!=0) result= sessionComp;
		}
		return result;
	}

	@Override
	public String toString() {
		String result= username;
		if(!sessionId.equals("")) result += ("," + sessionId);
		return result;
	}
}
