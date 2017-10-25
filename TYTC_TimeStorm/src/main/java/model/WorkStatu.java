package model;

import java.io.Serializable;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WorkStatu implements Serializable{
	
	private static final long serialVersionUID = 1L;

	@JsonProperty(value = "RcvTime") 
	private String rcvTime;
	
	@JsonProperty(value = "Imei") 
	private String imei;
	
	@JsonProperty(value = "MsgTime") 
	private String msgTime;
	
	@JsonProperty(value = "PgnId") 
	private String pgnId;
	
	@JsonProperty(value = "ListItem")
	private HashMap<String, String> listItem;
		
	public String getRcvTime() {
		return rcvTime;
	}
	public void setRcvTime(String rcvTime) {
		this.rcvTime = rcvTime;
	}
	
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	
	public String getMsgTime() {
		return msgTime;
	}
	public void setMsgTime(String msgTime) {
		this.msgTime = msgTime;
	}
	
	public String getPgnId() {
		return pgnId;
	}
	public void setPgnId(String pgnId) {
		this.pgnId = pgnId;
	}
	
	public HashMap<String, String> getListItem() {
		return listItem;
	}
	public void setListItem(HashMap<String, String> listItem) {
		this.listItem = listItem;
	}
	@Override
	public String toString() {
		return "WorkStatu [rcvTime=" + rcvTime + ", imei=" + imei + ", msgTime=" + msgTime + ", pgnId=" + pgnId
				+ ", listItem=" + listItem + "]";
	}
	
	
	
	
}
