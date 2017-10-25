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

public class ElectricBolt extends BaseRichBolt {

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
			String rcvtime=workStatu.getRcvTime();
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(Time, rcvtime)) {
				LogInfo.appendLog("Electric.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");
				
			}
			
			String lastKeyFormat = "Last_" + Imei;
			
			Map<String, String> map = workStatu.getListItem();
			String electricCurrent = map.get("155");			
			Map<String, String> hash = new HashMap<String, String>();
			hash.put(Redis.ELE, electricCurrent);
			hash.put(Redis.ELE_Time, Time);
			SentinelJedisUtil.setHMSet(lastKeyFormat, hash);

			_collector.ack(input);
//			LogInfo.appendLog("ElectricBolt", workStatu.toString());

		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("ElectricBolt.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Electric"));
	}

}
