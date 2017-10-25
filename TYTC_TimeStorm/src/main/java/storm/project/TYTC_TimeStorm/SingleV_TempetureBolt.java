package storm.project.TYTC_TimeStorm;

import java.util.HashMap;
import java.util.Iterator;
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
import model.WorkStatu;

public class SingleV_TempetureBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {
		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			String Time = workStatu.getMsgTime();
			String Imei = workStatu.getImei();
			String currentRcvTime = workStatu.getRcvTime();
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(Time, currentRcvTime)) {
				LogInfo.appendLog("SingleV_Tempe.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");

			}

			String lastKeyFormat = "Last_" + Imei;

			Map<String, String> hash = new HashMap<String, String>();

			Map<String, String> map = workStatu.getListItem();
			Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				if (entry.getKey().equals("151") || entry.getKey().equals("152") || entry.getKey().equals("153")
						|| entry.getKey().equals("154")) {
					hash.put("Temp" + (Integer.valueOf(entry.getKey()) - 150), entry.getValue());
					hash.put("TempTime", Time);
				} else {
					hash.put("Voltage" + (Integer.valueOf(entry.getKey()) - 102), entry.getValue());
					hash.put("VoltageTime", Time);
				}
			}
			SentinelJedisUtil.setHMSet(lastKeyFormat, hash);

			_collector.ack(input);
//			LogInfo.appendLog("SingleV_Tempeture", workStatu.toString());
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("SingleV_Tempeture.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("SingleV"));
	}

}
