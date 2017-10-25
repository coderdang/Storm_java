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

import Util_Redis.SentinelJedisUtil;
import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import Utils_Time.TimeUtil;
import model.Redis;
import model.WorkStatu;

public class JDQstatusBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple input) {
		try {
			WorkStatu workStatu = (WorkStatu) input.getValueByField("object");
			LogInfo.appendLog("JDQstatu.recv", workStatu.toString());
			Map<String, String> map = workStatu.getListItem();

			String currentImei = workStatu.getImei();

			String currentRcvTime = workStatu.getRcvTime();
			String currentTime = workStatu.getMsgTime();
			// 防止客户端时间异常超大
			if (Private_Utils.error_Client_Time(currentTime, currentRcvTime)) {
				LogInfo.appendLog("JDQstatu.err.Client_Time", workStatu.toString());
				throw new Exception("客户端时间与服务器时间间隔超过1小时或者时间值为null");

			}
			Date msgTime = TimeUtil.toDate(currentTime, "yyyy-MM-dd HH:mm:ss.SSS");

			String dayKeyFormat = "Day_" + currentImei + "_" + TimeUtil.toString(msgTime, "yyyyMMdd");// 信息生成时间格式化yyyyMMdd

			String lastKeyFormat = "Last_" + currentImei;

			String varKeyFormat = "var_" + currentImei;

			String current72 = map.get("72");// 主继电器状态 72

			String current73 = map.get("73");// 充电继电器状态 73

			// 从最新信息中读取tWork
			String tWorkRedis = SentinelJedisUtil.getHSet(lastKeyFormat, Redis.TWORK);
			Long tWorkLast = 0l;
			if (tWorkRedis != null) {
				tWorkLast = Long.parseLong(tWorkRedis);
			}

			// 从当天信息中读取当天开机时间
			String iWorkRedis = SentinelJedisUtil.getHSet(dayKeyFormat, Redis.IWORK);
			Long iWorkDay = 0l;
			if (iWorkRedis != null) {
				iWorkDay = Long.parseLong(iWorkRedis);
			}

			String relay = SentinelJedisUtil.getHSet(varKeyFormat, Redis.RELAY1);// 状态（0:主继电器断开

			String relaytime = SentinelJedisUtil.getHSet(varKeyFormat, Redis.RELAYTIME1);
			// 当期时间和上一次relaytime 区间值
			Long IntervalValue = Private_Utils.timeSubtract(currentTime, relaytime, "yyyy-MM-dd HH:mm:ss.SSS");// 区间值
			if (IntervalValue < 0) {
				return;
			}
			// 最新状态放进redis
			Map<String, String> hash = new HashMap<String, String>();
			hash.put(Redis.RELAY1, current72);
			hash.put(Redis.RELAYTIME1, currentTime);
			hash.put(Redis.RELAY2, current73);
			hash.put(Redis.RELAYTIME2, currentTime);
			SentinelJedisUtil.setHMSet(varKeyFormat, hash);
			// 1:主继电器闭合)
			if (relay != null) {

				if (relay.equals("1")) {// 上一状态是闭合
					if (current72.equals("1")) {
						LogInfo.appendLog("JDQstatu.inter"+currentImei,"iwork:"+iWorkDay);
						LogInfo.appendLog("JDQstatu.inter"+currentImei,"current:"+currentTime+" last:"+relaytime+" qujian:"+IntervalValue);
						// 截取当前继电器时间yyyy-MM-dd
						String currentTime1 = currentTime.substring(0, 10);
						// 截取上一次继电器时间yyyy-MM-dd
						String relaytime1 = relaytime.substring(0, 10);
						// 如果数据跨天，不累加，但是记录当前时间
						if (!currentTime1.equals(relaytime1)) {

							LogInfo.appendLog("JDQstatu.err.Otherday",
									workStatu.toString() + currentTime1 + relaytime1);
							hash = new HashMap<String, String>();

							hash.put(Redis.RELAY1, current72);
							hash.put(Redis.RELAYTIME1, currentTime);
							hash.put(Redis.RELAY2, current73);
							hash.put(Redis.RELAYTIME2, currentTime);
							SentinelJedisUtil.setHMSet(varKeyFormat, hash);
							// 7 days out
							SentinelJedisUtil.expire(dayKeyFormat, 24 * 60 * 60 * 7);

							hash = new HashMap<String, String>();
							hash.put("SwitchState", current72);
							hash.put("SwitchStateTime", currentTime);
							SentinelJedisUtil.setHMSet(lastKeyFormat, hash);
							return;
						}
						tWorkLast += IntervalValue;

						iWorkDay += IntervalValue;
						LogInfo.appendLog("JDQstatu.inter"+currentImei,"iwork:"+iWorkDay);
//						// 如果当天大于24小时，归为24小时
//						if (iWorkDay > 24 * 60 * 60 * 1000) {
//							iWorkDay = (long) (24 * 60 * 60 * 1000);
//						}
						hash = new HashMap<String, String>();
						hash.put(Redis.IWORK, iWorkDay.toString());
						hash.put(Redis.IWORKTIME, currentTime);
						hash.put(Redis.TWORK, tWorkLast.toString());
						hash.put(Redis.TWORKTIME, currentTime);

						SentinelJedisUtil.setHMSet(dayKeyFormat, hash);
						SentinelJedisUtil.setHMSet(lastKeyFormat, hash);

						// =============================计算停放时间===================================

						// 从最新读取累计停放时间
						String tParkingRedis = SentinelJedisUtil.getHSet(lastKeyFormat, "tParking");
						Long tparking = 0l;
						if (tParkingRedis != null) {
							tparking = Long.parseLong(tParkingRedis);
						}
						// 从当天信息读取当天停放时间
						String iparkingRedis = SentinelJedisUtil.getHSet(dayKeyFormat, "iParking");
						Long iparking = 0l;
						if (iparkingRedis != null) {
							iparking = Long.parseLong(iparkingRedis);
						}
						// 从最新获得电流及时间
						String electric = SentinelJedisUtil.getHSet(lastKeyFormat, Redis.ELE);
						String electric_time = SentinelJedisUtil.getHSet(lastKeyFormat, Redis.ELE_Time);

						if (electric != null) {
							if (currentTime != null) {

								// 当前信息时间 - 上一条内存中的电流时间，比较是不是最近时间的
								Long time = Private_Utils.timeSubtract(currentTime, electric_time,
										"yyyy-MM-dd HH:mm:ss.SSS");

								// 最新电流值时间<3分钟且电流<2A则认为停留
								if (Double.parseDouble(electric) < 2.0 && Math.abs(time) < 60 * 60 * 1000) {

									tparking += IntervalValue;
									iparking += IntervalValue;

									hash = new HashMap<String, String>();
									hash.put("iParking", iparking.toString());
									hash.put("iParkingTime", electric_time);
									hash.put("tParking", tparking.toString());
									hash.put("tParkingTime", electric_time);

									SentinelJedisUtil.setHMSet(dayKeyFormat, hash);
									SentinelJedisUtil.setHMSet(lastKeyFormat, hash);
								}
							}
						}
					}
				}
			}

			hash = new HashMap<String, String>();
			hash.put("SwitchState", current72);
			hash.put("SwitchStateTime", currentTime);
			SentinelJedisUtil.setHMSet(lastKeyFormat, hash);
			// 7 days out
			SentinelJedisUtil.expire(dayKeyFormat, 24 * 60 * 60 * 7);
			_collector.ack(input);
			LogInfo.appendLog("jdq", workStatu.toString());
		} catch (Exception e) {
			_collector.fail(input);
			e.printStackTrace();
			LogInfo.appendLog("jdq.err", PrintException.getStackTrace(e));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("JDQstatus"));
	}

}
