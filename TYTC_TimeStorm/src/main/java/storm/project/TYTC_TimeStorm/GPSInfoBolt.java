package storm.project.TYTC_TimeStorm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import Util_Redis.SentinelJedisUtil;
import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import model.Redis;
import model.WorkStatu;

public class GPSInfoBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {
		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			String Imei = workStatu.getImei();
			String Time=workStatu.getMsgTime();
			String rcvtime=workStatu.getRcvTime();
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(Time, rcvtime)) {
				LogInfo.appendLog("GPS.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");
				
			}
			Map<String, String> map = workStatu.getListItem();
			String la = map.get("48");
			String lo = map.get("50");
			String speed = map.get("51");
			String direction = map.get("52");
			String new_53=map.get("53");
			String altitude = map.get("55");
			String satellite = map.get("56");			
			String pos_time = map.get("57");
			//开机没定位
			if(new_53.equals("0")){
				LogInfo.appendLog("GPS.err.noPosition", workStatu.toString());
				return;
			}

			Map<String, String> hash = new HashMap<String, String>();
			hash.put(Redis.LO, lo);
			hash.put(Redis.LA, la);
			hash.put(Redis.SPEED, speed);
			hash.put(Redis.DIRECTION, direction);
			hash.put(Redis.POS_time, pos_time);
			hash.put(Redis.ALTITUDE, altitude);
			hash.put(Redis.SATELLITE, satellite);
			SentinelJedisUtil.setHMSet("Last_" + Imei, hash);
			_collector.ack(input);
//			LogInfo.appendLog("GPSinfor", workStatu.toString());
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("GPSinfor.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("GPSInfo"));
	}

}
