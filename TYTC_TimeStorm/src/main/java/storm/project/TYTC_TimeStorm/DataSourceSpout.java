package storm.project.TYTC_TimeStorm;

import java.net.InetAddress;
import java.util.Arrays;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import org.apache.storm.Config;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import Utils_Time.LogInfo;
import Utils_Time.PrintException;

public class DataSourceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(DataSourceSpout.class);

	boolean _isDistributed;
	private SpoutOutputCollector _collector;
	private Properties props;
	private KafkaConsumer<String, String> consumer;
	String computername = null;

	public DataSourceSpout() {
		this(true);
		try {
			computername = InetAddress.getLocalHost().getHostAddress() + ":" + System.currentTimeMillis();
		} catch (Exception e) {
			System.out.println("Exception caught = " + e.getMessage());
		}
	}

	public DataSourceSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		try {
			this._collector = collector;
			// 配置kfk消费者参数
			props = new Properties();
			props.load(this.getClass().getResourceAsStream("/Config/kafka_consumer.properties"));
			consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList("TYTC_Work"));// topic can more
		} catch (Exception e) {
			LogInfo.appendLog("DataSourceSpout.open.err", PrintException.getStackTrace(e));
		}
	}

	public void nextTuple() {
		try {
			ConsumerRecords<String, String> records = consumer.poll(1000);// 每次拉取时间100ms,最多拉取1000条

//			 LOG.info("______Kafka(V0.10.x)ConsumerDemo--->consumerMessage--->"
//			 +
//			 "records.count==>" + +records.count());

			for (ConsumerRecord<String, String> record : records) {
				System.out.println("record.partition()==>" + record.partition() + " record.offset()==>"
						+ record.offset() + "   key==>" + record.key() + "   val==>" + record.value());

//				LogInfo.appendLog("Data_from_kfk", record.value());

				_collector.emit(new Values(record.value()));

			}
		} catch (Exception e) {
			LogInfo.appendLog("DataSourceSpout.nextTuple.err", PrintException.getStackTrace(e));
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("work_source"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

}
