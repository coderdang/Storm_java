package storm.project.TYTC_TimeStorm;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
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
import Utils_Time.TimeUtil;
import model.DataBean;
import model.Redis;
import model.WorkStatu;

public class TotalV_SOCBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {

		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			String Imei = workStatu.getImei();
			String rec_time = workStatu.getRcvTime();
			String currentTime = workStatu.getMsgTime();

			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(currentTime, rec_time)) {
				LogInfo.appendLog("TotalV_SOC.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");

			}
			Date msgTime = TimeUtil.toDate(currentTime, "yyyy-MM-dd HH:mm:ss.SSS");
			String dayKeyFormat = "Day_" + Imei + "_" + TimeUtil.toString(msgTime, "yyyyMMdd");// 信息生成时间格式化yyyyMMdd

			String lastKeyFormat = "Last_" + Imei;

			Map<String, String> map = workStatu.getListItem();
			String totalV = map.get("160");
			String SOC = map.get("161");

			/** soc当天累计上升百分比 */
			Double iCSOC = 0.0;
			String iCSOC_Redis = SentinelJedisUtil.getHSet(dayKeyFormat, "SocRise");
			if (iCSOC_Redis != null) {
				iCSOC = Double.parseDouble(iCSOC_Redis);
			}

			/** soc当天累计下降百分比 */
			Double iDSOC = 0.0;
			String iDSOC_Redis = SentinelJedisUtil.getHSet(dayKeyFormat, "SocDecline");
			if (iDSOC_Redis != null) {
				iDSOC = Double.parseDouble(iDSOC_Redis);
			}

			/** soc累计上升百分比 */
			Double tCSOC = 0.0;
			String tCSOC_Redis = SentinelJedisUtil.getHSet(lastKeyFormat, "tSocRise");
			if (tCSOC_Redis != null) {
				tCSOC = Double.parseDouble(tCSOC_Redis);
			}

			/** soc累计下降百分比 */
			Double tDSOC = 0.0;
			String tDSOC_Redis = SentinelJedisUtil.getHSet(lastKeyFormat, "tSocDecline");
			if (tDSOC_Redis != null) {
				tDSOC = Double.parseDouble(tDSOC_Redis);
			}

			/** 累计使用电量 */
			Double tConsumeQuantity = 0.0;
			String tConsumeQuantity_Redis = SentinelJedisUtil.getHSet(lastKeyFormat, "tConsumeQuantity");
			if (tConsumeQuantity_Redis != null) {
				tConsumeQuantity = Double.parseDouble(tConsumeQuantity_Redis);
			}

			/** 当天充电电量 */
			Double iChargeQuantity = 0.0;
			String iChargeQuantity_Redis = SentinelJedisUtil.getHSet(dayKeyFormat, "iChargeQuantity");
			if (iChargeQuantity_Redis != null) {
				iChargeQuantity = Double.parseDouble(iChargeQuantity_Redis);
			}

			/** 当天使用电量 */
			Double iConsumeQuantity = 0.0;
			String iConsumeQuantity_Redis = SentinelJedisUtil.getHSet(dayKeyFormat, "iConsumeQuantity");
			if (iConsumeQuantity_Redis != null) {
				iConsumeQuantity = Double.parseDouble(iConsumeQuantity_Redis);
			}

			if (SOC != null) {

				// region 判断报警逻辑

				// 报警
				String SocLast_Redis = SentinelJedisUtil.getHSet(lastKeyFormat, "Soc");
				if (new BigDecimal(SOC).doubleValue() < 10) {

				

					SentinelJedisUtil.setHSet("Alarm_" + Imei, "Alarm1", "1");
					SentinelJedisUtil.setHSet("Alarm_" + Imei, "Alarm1Time", currentTime);

					DataBean dataBean = Private_Utils.sendAlarmReport(rec_time, Imei, currentTime, "1", "1");
					_collector.emit(new Values(dataBean));
					// }
				}
				// 报警解除
				if (SocLast_Redis != null) {
					if (new BigDecimal(SocLast_Redis).doubleValue() < 10) {
						if (new BigDecimal(SOC).doubleValue() > 10) {

							SentinelJedisUtil.setHSet("Alarm_" + Imei, "Alarm1", "0");
							SentinelJedisUtil.setHSet("Alarm_" + Imei, "Alarm1Time", currentTime);

							DataBean dataBean = Private_Utils.sendAlarmReport(rec_time, Imei, currentTime, "0", "1");
							_collector.emit(new Values(dataBean));
						}
					}
				}
				
				//当前的soc更新到redis
				Map<String, String> hash = new HashMap<String, String>();
				hash.put("Soc", SOC);
				hash.put("SocTime", currentTime);
				hash.put("tVoltage", totalV);
				hash.put("tVoltageTime", currentTime);
				SentinelJedisUtil.setHMSet(lastKeyFormat, hash);
				// region 电池使用/充电

				// 电池容量
				String BatteryCapacity = SentinelJedisUtil.getHSet(lastKeyFormat, Redis.BATTERTYCAPACITY);
				// 上升
				if (SocLast_Redis != null) {
					if (((new BigDecimal(SOC).subtract(new BigDecimal(SocLast_Redis))).doubleValue()) > 0) {

						tCSOC = (new BigDecimal(tCSOC.toString())
								.add(new BigDecimal(SOC).subtract(new BigDecimal(SocLast_Redis)))).doubleValue();
						SentinelJedisUtil.setHSet(lastKeyFormat, "tSocRise", tCSOC.toString());
						SentinelJedisUtil.setHSet(lastKeyFormat, "tSocRiseTime", currentTime);

						iCSOC = (new BigDecimal(iCSOC.toString())
								.add(new BigDecimal(SOC).subtract(new BigDecimal(SocLast_Redis)))).doubleValue();
						SentinelJedisUtil.setHSet(dayKeyFormat, "SocRise", iCSOC.toString());
						SentinelJedisUtil.setHSet(dayKeyFormat, "SocRiseTime", currentTime);

						if (BatteryCapacity != null) {
							// 当天chongdian电量
							iChargeQuantity = (new BigDecimal(BatteryCapacity)
									.multiply(new BigDecimal(iCSOC.toString()))).divide(new BigDecimal("100"))
											.doubleValue();

							SentinelJedisUtil.setHSet(dayKeyFormat, "iChargeQuantity", iChargeQuantity.toString());
							SentinelJedisUtil.setHSet(lastKeyFormat, "iChargeQuantity", iChargeQuantity.toString());
							SentinelJedisUtil.setHSet(dayKeyFormat, "BatteryCapacity", BatteryCapacity);
						}
					} else if (((new BigDecimal(SOC).subtract(new BigDecimal(SocLast_Redis))).doubleValue()) < 0) {

						tDSOC = (new BigDecimal(tDSOC.toString())
								.add(new BigDecimal(SocLast_Redis).subtract(new BigDecimal(SOC)))).doubleValue();
						SentinelJedisUtil.setHSet(lastKeyFormat, "tSocDecline", tDSOC.toString());
						SentinelJedisUtil.setHSet(lastKeyFormat, "tSocDeclineTime", currentTime);

						iDSOC = (new BigDecimal(iDSOC.toString())
								.add(new BigDecimal(SocLast_Redis).subtract(new BigDecimal(SOC)))).doubleValue();
						SentinelJedisUtil.setHSet(dayKeyFormat, "SocDecline", iDSOC.toString());
						SentinelJedisUtil.setHSet(dayKeyFormat, "SocDeclineTime", currentTime);

						if (BatteryCapacity != null) {
							// 当天使用电量
							iConsumeQuantity = (new BigDecimal(BatteryCapacity)
									.multiply(new BigDecimal(iDSOC.toString()))).divide(new BigDecimal("100"))
											.doubleValue();
							SentinelJedisUtil.setHSet(dayKeyFormat, "iConsumeQuantity", iConsumeQuantity.toString());
							SentinelJedisUtil.setHSet(lastKeyFormat, "iConsumeQuantity", iConsumeQuantity.toString());

							// 累计使用电量
							tConsumeQuantity = (new BigDecimal(BatteryCapacity)
									.multiply(new BigDecimal(tDSOC.toString()))).divide(new BigDecimal("100"))
											.doubleValue();
							SentinelJedisUtil.setHSet(lastKeyFormat, "tConsumeQuantity", tConsumeQuantity.toString());
							SentinelJedisUtil.setHSet(lastKeyFormat, "tConsumeQuantityTime", currentTime);

							SentinelJedisUtil.setHSet(dayKeyFormat, "tConsumeQuantity", tConsumeQuantity.toString());
							SentinelJedisUtil.setHSet(dayKeyFormat, "tConsumeQuantityTime", currentTime);
						}
					}
				}

				// region 实时电量

				// 电池容量 * SOC
				if (BatteryCapacity != null) {

					Double electricity = (new BigDecimal(BatteryCapacity).multiply(new BigDecimal(SOC.toString())))
							.divide(new BigDecimal("100")).doubleValue();
					SentinelJedisUtil.setHSet(lastKeyFormat, "Electricity", electricity.toString());
					SentinelJedisUtil.setHSet(lastKeyFormat, "ElectricityTime", currentTime);
				}

			}

			

			LogInfo.appendLog("TotalV_SOC", workStatu.toString());

			// 7 days out
			SentinelJedisUtil.expire(dayKeyFormat, 24 * 60 * 60 * 7);

			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("TotalV_SOC.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Object"));
	}

}
