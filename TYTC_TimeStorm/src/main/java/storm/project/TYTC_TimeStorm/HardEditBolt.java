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

public class HardEditBolt extends BaseRichBolt {

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
			String rcvtime=workStatu.getRcvTime();
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(currentTime, rcvtime)) {
				LogInfo.appendLog("HardEdit.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");
				
			}
			
			Date msgTime = TimeUtil.toDate(currentTime,"yyyy-MM-dd HH:mm:ss.SSS");
			String dayKeyFormat = "Day_" + Imei + "_" + TimeUtil.toString(msgTime,"yyyyMMdd");// 信息生成时间格式化yyyyMMdd
			String lastKeyFormat = "Last_" + Imei;
			
			Map<String, String> map = workStatu.getListItem();
			String softEdition = map.get("163");
			String hardEdition = map.get("164");
			
			Map<String, String> hash = new HashMap<String, String>();
			hash.put(Redis.HARD_EDIT, hardEdition);
			hash.put(Redis.HARD_EDIT_Time, currentTime);
			hash.put(Redis.SOF_EDIT, softEdition);
			hash.put(Redis.SOF_EDIT_Time, currentTime);
			
			SentinelJedisUtil.setHMSet(dayKeyFormat, hash);
			SentinelJedisUtil.setHMSet(lastKeyFormat, hash);

			// 7 days out
			SentinelJedisUtil.expire(dayKeyFormat, 24 * 60 * 60 * 7);

//			LogInfo.appendLog("Hard_softEdit", workStatu.toString());
			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("Hard_softEdit.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("HardEdit"));
	}

}
