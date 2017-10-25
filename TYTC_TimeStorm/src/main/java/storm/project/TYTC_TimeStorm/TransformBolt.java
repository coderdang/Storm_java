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

public class TransformBolt extends BaseRichBolt {
	private OutputCollector _collector;
	// jackjson 实例对象
	private ObjectMapper mapper;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		try {
			this._collector = collector;
			mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogInfo.appendLog("TransformBolt.prepare.err", PrintException.getStackTrace(e));
		}
	}

	public void execute(Tuple input) {
		try {
			String source = input.getString(0);
			// json字符串转化成TYTC_Source对象
			WorkStatu workStatu = mapper.readValue(source, WorkStatu.class);
			_collector.emit(new Values(workStatu));
			
			_collector.ack(input);
			
			LogInfo.appendLog("Transform", source);
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();			
			LogInfo.appendLog("Transform.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Object1"));
	}

}
