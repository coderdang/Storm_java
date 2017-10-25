package storm.project.TYTC_TimeStorm;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import model.WorkStatu;

public class ClassifyBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;

	private OutputCollector _collector;	
	// jackjson 实例对象
	private ObjectMapper mapper = new ObjectMapper();	

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this._collector = collector;
		mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public void execute(Tuple input) {

		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("Object1");
			String pgnid = workStatu.getPgnId();			
			switch (pgnid) {
			case "9":

				System.out.println("GPS信息");
				_collector.emit("str_1", new Values(workStatu));
				
				break;
			case "13":

				System.out.println("继电器状态");
				_collector.emit("str_2", new Values(workStatu));
				break;
			case "14":

				System.out.println("充放电状态");
				_collector.emit("str_3", new Values(workStatu));
				break;
			case "15":

				System.out.println("故障代码");
				_collector.emit("str_4", new Values(workStatu));
				break;
			case "16":

				System.out.println("系统编号，标称AH");
				_collector.emit("str_5", new Values(workStatu));
				break;
			case "17":

				System.out.println("单体电压，采样温度");
				_collector.emit("str_6", new Values(workStatu));
				break;
			case "18":

				System.out.println("电流");
				_collector.emit("str_7", new Values(workStatu));
				break;
			case "19":

				System.out.println("温度最高、最低、单体最高、单体最低");
				_collector.emit("str_11", new Values(workStatu));
				break;
			case "20":

				System.out.println("总电压，SOC");
				_collector.emit("str_8", new Values(workStatu));
				break;
			case "21":

				System.out.println("充电次数");
				_collector.emit("str_9", new Values(workStatu));
				break;
			case "22":

				System.out.println("软硬件版本");
				_collector.emit("str_10", new Values(workStatu));
				break;
			}
			
			_collector.ack(input);
			
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();			
			LogInfo.appendLog("ClassifyBolt.err", PrintException.getStackTrace(e));
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("str_1", new Fields("object"));
		declarer.declareStream("str_2", new Fields("object"));
		declarer.declareStream("str_3", new Fields("object"));
		declarer.declareStream("str_4", new Fields("object"));
		declarer.declareStream("str_5", new Fields("object"));
		declarer.declareStream("str_6", new Fields("object"));
		declarer.declareStream("str_7", new Fields("object"));
		declarer.declareStream("str_8", new Fields("object"));
		declarer.declareStream("str_9", new Fields("object"));
		declarer.declareStream("str_10", new Fields("object"));
		declarer.declareStream("str_11", new Fields("object"));
	}
}
