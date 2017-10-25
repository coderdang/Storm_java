package storm.project.TYTC_TimeStorm;

import java.util.Date;
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
import Utils_Time.TimeUtil;
import model.Redis;
import model.WorkStatu;

/*
 * 温度最高、最低、单体最高、单体最低
 */
public class SingleLimitBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {
		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			String Imei = workStatu.getImei();
			String currentTime = workStatu.getMsgTime();
			String currentRcvTime=workStatu.getRcvTime();					
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(currentTime, currentRcvTime)) {
				LogInfo.appendLog("SingleLimit.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");
				
			}
			
			Date msgTime = TimeUtil.toDate(currentTime,"yyyy-MM-dd HH:mm:ss.SSS");
			String dayKeyFormat = "Day_" + Imei + "_" + TimeUtil.toString(msgTime,"yyyyMMdd");// 信息生成时间格式化yyyyMMdd			

			Map<String, String> map = workStatu.getListItem();
			String SingleVoltageMax = map.get("156");//单体电压最大值
			String SingleVoltageMin = map.get("157");//单体电压最小值
			String SingleTempMax = map.get("158");//单体温度最大值
			String SingleTempMin = map.get("159");//单体温度最小值

			Map<String, String> hash = new HashMap<String, String>();
			hash.put(Redis.SingleVoltageMax, SingleVoltageMax);
			hash.put(Redis.SingleVoltageMin, SingleVoltageMin);
			hash.put(Redis.SingleTempMax, SingleTempMax);
			hash.put(Redis.SingleTempMin, SingleTempMin);
			hash.put(Redis.SingleTime, currentTime);
			
			SentinelJedisUtil.setHMSet(dayKeyFormat, hash);
			
			// 7 days out
			SentinelJedisUtil.expire(dayKeyFormat, 24 * 60 * 60 * 7);

			_collector.ack(input);
//			LogInfo.appendLog("SingleLimit", workStatu.toString());
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();			
			LogInfo.appendLog("SingleLimit.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("SingleLimit"));
	}
}
