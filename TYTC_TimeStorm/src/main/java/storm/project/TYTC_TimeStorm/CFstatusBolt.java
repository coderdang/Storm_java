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
import org.apache.storm.tuple.Values;
import Util_Redis.SentinelJedisUtil;
import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import Utils_Time.TimeUtil;
import model.DataBean;
import model.WorkStatu;

/**
 * 充放电状态
 */
public class CFstatusBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {
		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			String currentImei = workStatu.getImei();
			String RcvTime = workStatu.getRcvTime();
			String currentTime = workStatu.getMsgTime();

			// 客户端时间与服务器时间间隔超过1小时
			if (Private_Utils.error_Client_Time(currentTime, RcvTime)) {
				LogInfo.appendLog("CFstatu.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");

			}

			Date msgTime = TimeUtil.toDate(currentTime, "yyyy-MM-dd HH:mm:ss.SSS");
			String dayKeyFormat = "Day_" + currentImei + "_" + TimeUtil.toString(msgTime, "yyyyMMdd");// 信息生成时间格式化yyyyMMdd
			String lastKeyFormat = "Last_" + currentImei;
			String varKeyFormat = "var_" + currentImei;

			Map<String, String> map = workStatu.getListItem();
			String chargingState = map.get("74");

			// 累计充电时间
			String tChargingTimeRedis = SentinelJedisUtil.getHSet(lastKeyFormat, "tChargingTime");
			Long tcharging_time = 0l;
			if (tChargingTimeRedis != null) {
				tcharging_time = Long.parseLong(tChargingTimeRedis);
			}

			// 当天累计充电时间
			String daytcharging_timeRedis = SentinelJedisUtil.getHSet(dayKeyFormat, "iChargingTime");
			Long daytcharging_time = 0l;
			if (daytcharging_timeRedis != null) {
				daytcharging_time = Long.parseLong(daytcharging_timeRedis);
			}

			// 充电开始时间
			String chargingBeginTimeRedis = SentinelJedisUtil.getHSet(lastKeyFormat, "ChargingBeginTime");
			String chargingBeginTime = "";
			if (chargingBeginTimeRedis != null) {
				chargingBeginTime = chargingBeginTimeRedis;
			}

			// 放电电开始时间
			String dischargingBeginTimeRedis = SentinelJedisUtil.getHSet(lastKeyFormat, "disChargingBeginTime");
			String dischargingBeginTime = "";
			if (dischargingBeginTimeRedis != null) {
				dischargingBeginTime = dischargingBeginTimeRedis;
			}

			// 充放电状态变量
			String varChargingStateRedis = SentinelJedisUtil.getHSet(varKeyFormat, "ChargingState");

			String chargingStateTime = SentinelJedisUtil.getHSet(varKeyFormat, "ChargingStateTime");
			Long subTime = Private_Utils.timeSubtract(currentTime, chargingStateTime, "yyyy-MM-dd HH:mm:ss.SSS");
			if (subTime < 0) {
				LogInfo.appendLog("CFstatu.errOrder.Time", workStatu.toString());
				return;
			}
			// 充放电状态to var key
			SentinelJedisUtil.setHSet(varKeyFormat, "ChargingState", chargingState);
			SentinelJedisUtil.setHSet(varKeyFormat, "ChargingStateTime", currentTime);
			// 充放电状态to last key
			SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingState", chargingState);
			SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingStateTime", currentTime);

			// 截取当前时间年月日
			String currentTime1 = currentTime.substring(0, 10);
			// 截取上一次时间年月日
			String chargingStateTime1 = "";
			if (chargingStateTime != null) {
				chargingStateTime1 = chargingStateTime.substring(0, 10);
			}
			/********** 充电 **********/
			// 充电开始时间
			if (varChargingStateRedis == null || varChargingStateRedis.equals("1")
					|| varChargingStateRedis.equals("0")) {

				if (chargingState.equals("2")) {
					chargingBeginTime = currentTime;// 充电开始时间
					// 放到redis缓存
					SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingBeginTime", chargingBeginTime);
				}
			}
			if (varChargingStateRedis != null) {
				if (varChargingStateRedis.equals("2")) {
					/** 跨天充电报告 **/
					// 当前时间和和上一次时间yyyyMMdd不同认为是跨天
					if (!currentTime1.equals(chargingStateTime1)) {
						LogInfo.appendLog("cfstatu.err.Otherday",
								workStatu.toString() + currentTime1 + chargingStateTime1);
						String EndTime = chargingStateTime;
						DataBean dataBean = Private_Utils.sendChargingReport(RcvTime, currentImei, currentTime1,
								varChargingStateRedis, chargingBeginTime, EndTime);
						_collector.emit(new Values(dataBean));

						SentinelJedisUtil.setHSet(varKeyFormat, "ChargingState", chargingState);
						SentinelJedisUtil.setHSet(varKeyFormat, "ChargingStateTime", currentTime);

						SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingState", chargingState);
						SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingStateTime", currentTime);
						return;
					}

					if (chargingState.equals("2")) {

						// 当前时间和上一次充电时间差值IntervalValue
						Long IntervalValue = Private_Utils.timeSubtract(currentTime, chargingStateTime,
								"yyyy-MM-dd HH:mm:ss.SSS");
						if (IntervalValue < 0) {
							return;
						}

						tcharging_time += IntervalValue;
						daytcharging_time += IntervalValue;
//						// 如果当天大于24小时，归为24小时
//						if (daytcharging_time > 24 * 60 * 60 * 1000) {
//							daytcharging_time = (long) (24 * 60 * 60 * 1000);
//						}
						Map<String, String> hash = new HashMap<String, String>();
						hash.put("iChargingTime", daytcharging_time.toString());
						hash.put("iChargingTimeTime", currentTime);
						hash.put("tChargingTime", tcharging_time.toString());
						hash.put("tChargingTimeTime", currentTime);
						SentinelJedisUtil.setHMSet(dayKeyFormat, hash);
						SentinelJedisUtil.setHMSet(lastKeyFormat, hash);

						// 7 days out
						SentinelJedisUtil.expire(dayKeyFormat, 24 * 60 * 60 * 7);

					}

					/** 当天充电报告 **/
					if (chargingState.equals("1") || chargingState.equals("0")) {
						long endTime_int = ((TimeUtil.toDate(currentTime, "yyyy-MM-dd HH:mm:ss.SSS").getTime()) - 1000);
						String EndTime = TimeUtil.toString(new Date(endTime_int), "yyyy-MM-dd HH:mm:ss.SSS");
						DataBean dataBean = Private_Utils.sendChargingReport(RcvTime, currentImei, currentTime1,
								varChargingStateRedis, chargingBeginTime, EndTime);
						_collector.emit(new Values(dataBean));

					}
				}
			}
			/*********** 放电 ************/
			// 放电开始时间
			if (varChargingStateRedis == null || varChargingStateRedis.equals("2")
					|| varChargingStateRedis.equals("0")) {// 无或者是0,2

				if (chargingState.equals("1")) {
					dischargingBeginTime = currentTime;// 放电开始时间

					SentinelJedisUtil.setHSet(lastKeyFormat, "disChargingBeginTime", dischargingBeginTime);
				}
			}
			// 没有时间累加，不用考虑时间
			if (varChargingStateRedis != null) {
				if (varChargingStateRedis.equals("1")) {
					/** 跨天放电报告 **/
					if (!currentTime1.equals(chargingStateTime1)) {
						LogInfo.appendLog("cfstatu.Otherday", workStatu.toString() + currentTime1 + chargingStateTime1);
						String EndTime = chargingStateTime;
						DataBean dataBean = Private_Utils.sendChargingReport(RcvTime, currentImei, currentTime1,
								varChargingStateRedis, dischargingBeginTime, EndTime);
						_collector.emit(new Values(dataBean));

						SentinelJedisUtil.setHSet(varKeyFormat, "ChargingState", chargingState);
						SentinelJedisUtil.setHSet(varKeyFormat, "ChargingStateTime", currentTime);

						SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingState", chargingState);
						SentinelJedisUtil.setHSet(lastKeyFormat, "ChargingStateTime", currentTime);
						return;

					}

					/** 当天放电报告 **/
					if (chargingState.equals("2") || chargingState.equals("0")) {// 保留意见
																					// ，应该加上0的或判断
						long endTime_int = ((TimeUtil.toDate(currentTime, "yyyy-MM-dd HH:mm:ss.SSS").getTime()) - 1000);
						String EndTime = TimeUtil.toString(new Date(endTime_int), "yyyy-MM-dd HH:mm:ss.SSS");
						DataBean dataBean = Private_Utils.sendChargingReport(RcvTime, currentImei, currentTime1,
								varChargingStateRedis, dischargingBeginTime, EndTime);
						_collector.emit(new Values(dataBean));
					}

				}
			}

			LogInfo.appendLog("CFstatu", workStatu.toString());

			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("CFstatu.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Object"));
	}

	// public

}
