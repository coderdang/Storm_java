package storm.project.TYTC_TimeStorm;

import java.util.Iterator;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import Util_Redis.SentinelJedisUtil;
import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import model.DataBean;
import model.WorkStatu;

public class BrokenBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {

		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			String Imei = workStatu.getImei();
			String Time = workStatu.getMsgTime();
			String rcvtime = workStatu.getRcvTime();
			Map<String, String> map = workStatu.getListItem();
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(Time, rcvtime)) {
				LogInfo.appendLog("Broken.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");

			}
			Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
				switch (entry.getKey()) {

				// region 75
				case "75":
					// 发生故障
					String fault1 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault1");
					if (entry.getValue().equals("1")) {
						if (fault1 == null || fault1.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault1", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault1Time", Time);

							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "6");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault1 != null) {
						if (fault1.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault1");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault1Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "6");

								_collector.emit(new Values(dataBean));

							}
						}
					}

					break;
				// endregion

				// region 76
				case "76":
					// 发生故障
					String fault2 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault2");
					if (entry.getValue().equals("1")) {
						if (fault2 == null || fault2.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault2", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault2Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "7");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault2 != null) {

						if (fault2.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault2");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault2Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "7");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 77
				case "77":
					// 发生故障
					String fault3 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault3");
					if (entry.getValue().equals("1")) {
						if (fault3 == null || fault3.equals("1")) {

							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "8");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault3 != null) {

						if (fault3.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault3");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault3Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "8");

								_collector.emit(new Values(dataBean));

							}
						}
					}

					break;
				// endregion

				// region 78
				case "78":
					// 发生故障
					String fault4 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault4");
					if (entry.getValue().equals("1")) {
						if (fault4 == null || fault4.equals("1")) {

							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "9");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault4 != null) {

						if (fault4.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault4");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault4Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "9");

								_collector.emit(new Values(dataBean));

							}
						}
					}

					break;
				// endregion

				// region 79
				case "79":
					// 发生故障
					String fault5 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault5");
					if (entry.getValue().equals("1")) {
						if (fault5 == null || fault5.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault5", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault5Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "10");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault5 != null) {

						if (fault5.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault5");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault5Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "10");

								_collector.emit(new Values(dataBean));

							}
						}
					}

					break;
				// endregion

				// region 80
				case "80":

					// 发生故障
					String fault7 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault7");
					if (entry.getValue().equals("1")) {
						if (fault7 == null || fault7.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault7", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault7Time", Time);

							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "12");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault7 != null) {

						if (fault7.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault7");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault7Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "12");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 81
				case "81":

					// 发生故障
					String fault8 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault8");
					if (entry.getValue().equals("1")) {
						if (fault8 == null || fault8.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault8", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault8Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "13");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault8 != null) {

						if (fault8.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault8");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault8Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "13");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 82
				case "82":
					// 发生故障
					String fault9 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault9");
					if (entry.getValue().equals("1")) {
						if (fault9 == null || fault9.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault9", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault9Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "14");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault9 != null) {

						if (fault9.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault9");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault9Time");
								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "14");

								_collector.emit(new Values(dataBean));

							}
						}
					}

					break;
				// endregion

				// region 83
				case "83":
					// 发生故障
					String fault10 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault10");
					if (entry.getValue().equals("1")) {
						if (fault10 == null || fault10.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault10", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault10Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "15");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault10 != null) {

						if (fault10.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault10");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault10Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "15");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 84
				case "84":
					// 发生故障
					String fault11 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault11");
					if (entry.getValue().equals("1")) {
						if (fault11 == null || fault11.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault11", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault11Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "21");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault11 != null) {

						if (fault11.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault11");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault11Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "21");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 85
				case "85":
					// 发生故障
					String fault12 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault12");
					if (entry.getValue().equals("1")) {
						if (fault12 == null || fault12.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault12", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault12Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "36");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault12 != null) {

						if (fault12.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault12");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault12Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "36");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 86
				case "86":
					// 发生故障
					String fault13 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault13");
					if (entry.getValue().equals("1")) {
						if (fault13 == null || fault13.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault13", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault13Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "37");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault13 != null) {

						if (fault13.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault13");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault13Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "37");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 87
				case "87":
					// 发生故障
					String fault14 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault14");
					if (entry.getValue().equals("1")) {
						if (fault14 == null || fault14.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault14", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault14Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "38");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault14 != null) {

						if (fault14.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault14");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault14Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "38");
								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 88
				case "88":
					// 发生故障
					String fault17 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault17");
					if (entry.getValue().equals("1")) {
						if (fault17 == null || fault17.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault17", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault17Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "52");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault17 != null) {

						if (fault17.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault17");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault17Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "52");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 89
				case "89":
					// 发生故障
					String fault18 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault18");
					if (entry.getValue().equals("1")) {
						if (fault18 == null || fault18.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault18", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault18Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "70");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault18 != null) {
						if (fault18.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault18");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault18Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "70");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 90
				case "90":
					// 发生故障
					String fault19 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault19");
					if (entry.getValue().equals("1")) {
						if (fault19 == null || fault19.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault19", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault19Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "71");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault19 != null) {

						if (fault19.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault19");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault19Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "71");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 91
				case "91":
					// 发生故障
					String fault20 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault20");
					if (entry.getValue().equals("1")) {
						if (fault20 == null || fault20.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault20", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault20Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "75");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault20 != null) {

						if (fault20.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault20");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault20Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "75");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 92
				case "92":
					// 发生故障
					String fault22 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault22");
					if (entry.getValue().equals("1")) {
						if (fault22 == null || fault22.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault22", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault22Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "77");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault22 != null) {

						if (fault22.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault22");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault22Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "77");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 93
				case "93":
					// 发生故障
					String fault23 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault23");
					if (entry.getValue().equals("1")) {
						if (fault23 == null || fault23.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault23", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault23Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "78");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault23 != null) {

						if (fault23.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault23");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault23Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "78");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 94
				case "94":
					// 发生故障
					String fault24 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault24");
					if (entry.getValue().equals("1")) {
						if (fault24 == null || fault24.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault24", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault24Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "79");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault24 != null) {

						if (fault24.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault24");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault24Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "79");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 95
				case "95":
					// 发生故障
					String fault25 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault25");
					if (entry.getValue().equals("1")) {
						if (fault25 == null || fault25.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault25", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault25Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "80");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault25 != null) {

						if (fault25.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault25");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault25Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "80");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 96
				case "96":
					// 发生故障
					String fault26 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault26");
					if (entry.getValue().equals("1")) {
						if (fault26 == null || fault26.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault26", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault26Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "88");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault26 != null) {

						if (fault26.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault26");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault26Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "88");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 97
				case "97":
					// 发生故障
					String fault30 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault30");
					if (entry.getValue().equals("1")) {
						if (fault30 == null || fault30.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault30", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault30Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "111");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault30 != null) {

						if (fault30.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault30");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault30Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "111");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion

				// region 98
				case "98":
					// 发生故障
					String fault31 = SentinelJedisUtil.getHSet("Fault_" + Imei, "Fault31");
					if (entry.getValue().equals("1")) {
						if (fault31 == null || fault31.equals("1")) {

							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault31", entry.getValue());
							SentinelJedisUtil.setHSet("Fault_" + Imei, "Fault31Time", Time);
							DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "1", "113");

							_collector.emit(new Values(dataBean));
						}
					}
					// 故障解除
					if (fault31 != null) {

						if (fault31.equals("1")) {

							if (entry.getValue().equals("0")) {

								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault31");
								SentinelJedisUtil.delHSet("Fault_" + Imei, "Fault31Time");

								DataBean dataBean = Private_Utils.sendBrokenReport(rcvtime, Imei, Time, "0", "113");

								_collector.emit(new Values(dataBean));

							}
						}
					}
					break;
				// endregion
				}

			}
			_collector.ack(input);

		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("broken.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Object"));
	}

}
