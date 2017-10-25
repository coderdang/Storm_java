package storm.project.TYTC_TimeStorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import Utils_Time.LogInfo;
import Utils_Time.PrintException;

public class Topology {
	public static int logType = 2;// 集群模式

	public static void main(String[] args) throws Exception {
		try {
			TopologyBuilder builder = new TopologyBuilder();

			int n = 3; // 自定义Task数量

			builder.setSpout("Source", new DataSourceSpout(), n);
			// json字符串转化成TYTC_Source对象
			builder.setBolt("transform", new TransformBolt(), n).shuffleGrouping("Source");
			// 任务分配
			builder.setBolt("classify", new ClassifyBolt(), n).shuffleGrouping("transform");
			// GPS信息
			builder.setBolt("GPSInfo", new GPSInfoBolt(), n).shuffleGrouping("classify", "str_1");
			// 继电器状态
			builder.setBolt("JDQstatus", new JDQstatusBolt(), 6).shuffleGrouping("classify", "str_2");
			// 充放电状态
			builder.setBolt("CFstatus", new CFstatusBolt(), 6).shuffleGrouping("classify", "str_3");
			// 故障代码
			builder.setBolt("Broken", new BrokenBolt(), n).shuffleGrouping("classify", "str_4");
			// 系统编号，标称AH
			builder.setBolt("SystemNum", new SystemNumBolt(), n).shuffleGrouping("classify", "str_5");
			// 单体电压，采样温度
			builder.setBolt("SingleV_Tempeture", new SingleV_TempetureBolt(), n).shuffleGrouping("classify", "str_6");
			// 电流
			builder.setBolt("Electric", new ElectricBolt(), n).shuffleGrouping("classify", "str_7");
			// 总电压，SOC
			builder.setBolt("TotalV_SOC", new TotalV_SOCBolt(), 10).shuffleGrouping("classify", "str_8");
			// 充电次数
			builder.setBolt("CDTimes", new CDTimesBolt(), n).shuffleGrouping("classify", "str_9");
			// 软硬件版本
			builder.setBolt("HardEdit", new HardEditBolt(), n).shuffleGrouping("classify", "str_10");
			// 温度最高、最低、单体最高、单体最低
			builder.setBolt("SingleLimit", new SingleLimitBolt(), n).shuffleGrouping("classify", "str_11");

			builder.setBolt("StoreToKfk", new StoredBolt(), 6).fieldsGrouping("CFstatus", new Fields("Object"))
					.fieldsGrouping("Broken", new Fields("Object")).fieldsGrouping("TotalV_SOC", new Fields("Object"));

			Config conf = new Config();
			conf.setDebug(true);
			if (args != null && args.length > 0) {
				Topology.logType = 2;
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

			} else {
				//
				Topology.logType = 1;
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("test", conf, builder.createTopology());
				// Utils.sleep(300000000);
				// cluster.killTopology("test");
				// cluster.shutdown();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogInfo.appendLog("Topology.err", PrintException.getStackTrace(e));
		}
	}

}
