package storm.project.TYTC_TimeStorm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import Util_kfk.ProducerUtils;
import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import model.DataBean;

public class StoredBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L; 
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		ProducerUtils.initPropsCofig();
	}

	public void execute(Tuple input) {

		DataBean dataBean = (DataBean) input.getValueByField("Object");
		try {
			String topic = dataBean.getTopic();
			String key = dataBean.getKey();
			String value = dataBean.getValue();
			if (topic != null && key != null && value != null) {
				ProducerUtils.produceMessage(topic, key, value);
				LogInfo.appendLog("StoreageToKfk","Topic:"+topic+","+"Key:"+key+","+"Broken:"+value);
			}
			
			_collector.ack(input);

		} catch (Exception e) {
			e.printStackTrace();	
			_collector.fail(input);
			LogInfo.appendLog("Storeage.err", PrintException.getStackTrace(e));
		}		

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	

}
