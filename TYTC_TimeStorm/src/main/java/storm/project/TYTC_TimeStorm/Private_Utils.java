package storm.project.TYTC_TimeStorm;

import Utils_Time.TimeUtil;
import model.DataBean;

public class Private_Utils {
	
	public static Long timeSubtract(String time1, String time2, String timeFormate) {

		return (TimeUtil.toDate(time1, timeFormate).getTime()) - (TimeUtil.toDate(time2, timeFormate).getTime());

	}
	
	public static boolean error_Client_Time(String msgTime, String rcvTime) {
		if(msgTime==null||rcvTime==null){
			return true;
		}
		Long t = timeSubtract(msgTime, rcvTime, "yyyy-MM-dd HH:mm:ss.SSS");
		if (t > 60 * 60 * 1000) {
			return true;
		}
		return false;
	}
	public static DataBean sendChargingReport(String RcvTime,String currentImei,String currentTime,String varChargingStateRedis,String BeginTime,String EndTime){
		String charging_report_json = "{\"RcvTime\":\"" + RcvTime + "\",\"Imei\":\"" + currentImei
				+ "\",\"MsgTime\":\"" + currentTime + "\",\"State\":\"" + varChargingStateRedis
				+ "\",\"BegMsgTimeinTime\":\"" + BeginTime + "\",\"EndTime\":\"" + EndTime + "\"}";
		DataBean dataBean = new DataBean();
		dataBean.setTopic("TYTC_ChargingReport");
		dataBean.setKey("CFstatus");
		dataBean.setValue(charging_report_json);
		
		return dataBean;
	}
	
	public static DataBean sendBrokenReport(String rcvtime,String Imei,String Time,String State,String Code){
		String fault = "{\"RcvTime\":\"" + rcvtime + "\",\"Imei\":\"" + Imei + "\",\"MsgTime\":\"" + Time
				+ "\",\"State\":\""+State+"\",\"Code\":\""+Code+"\"}";		
		DataBean dataBean = new DataBean();
		dataBean.setValue(fault);
		dataBean.setTopic("TYTC_Fault");
		dataBean.setKey("Broken");		
		return dataBean;
	}
	
	public static DataBean sendAlarmReport(String rec_time,String Imei,String currentTime,String State,String Type){
		String alarm = "{\"RcvTime\":\"" + rec_time + "\",\"Imei\":\"" + Imei + "\",\"MsgTime\":\"" + currentTime
				+ "\",\"State\":\""+State+"\",\"Type\":\""+Type+"\"}";
		
		DataBean dataBean = new DataBean();
		dataBean.setValue(alarm);
		dataBean.setTopic("TYTC_Alarm");
		dataBean.setKey("TotalV_SOCBolt");	
		return dataBean;
	}
	
	
	

}
