package storm.project.TYTC_TimeStorm;


import Util_Redis.SentinelJedisUtil;


public class test {

	public static void main(String[] args) {
	
//		System.out.println(SentinelJedisUtil.getHMSet("Last_862631034775758",Redis.LO,Redis.LO,Redis.SPEED,Redis.DIRECTION,Redis.POS_time));
	
//		"Day_" + Imei + "_" + yesterday, "itOpenTime";
//		System.out.println(SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "iWork"));
//		System.out.println(SentinelJedisUtil.delHSet(	"Last_862631034775758", "tWork"));
//		System.out.println(SentinelJedisUtil.delHSet(	"var_862631034775758", "Relay1"));
//		System.out.println(SentinelJedisUtil.delHSet(	"var_862631034775758", "RelayTime1"));
//		System.out.println(SentinelJedisUtil.delHSet("var_862631034775758", "ChargingStateTime"));
//		System.out.println(SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "iChargingTime"));
//		System.out.println(SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "iChargingTimes"));
//		System.out.println(SentinelJedisUtil.delHSet(	"Last_862631034775758", "tChargingTimes"));
//		System.out.println(SentinelJedisUtil.delHSet(	"Last_862631034775758", "Soc"));
//		System.out.println("累计使用电量："+SentinelJedisUtil.delHSet(	"Last_862631034775758", "tConsumeQuantity"));
//		System.out.println("实时电量："+SentinelJedisUtil.delHSet(	"Last_862631034775758", "Electricity"));
//		/*********************soc****************************/
//		
//		
//		System.out.println("当天充电时间："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "iChargingTime"));
//	    System.out.println("当天充电电量："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "iChargeQuantity"));
//		System.out.println("当天使用电量："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "iConsumeQuantity"));
//		System.out.println("截止当天累计soc上升百分比："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "itSocRise"));
//		System.out.println("截止当天累计soc下降百分比："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "itSocDecline"));
//		System.out.println("soc上升百分比："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "SocRise"));
//		System.out.println("soc下降百分比："+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "SocDecline"));
//		SentinelJedisUtil
//		.delHSet("Last_862631034775758", "tSocRise");
//		SentinelJedisUtil
//		.delHSet("Last_862631034775758", "tSocDecline");
		
//		System.out.println("继电器状态："+SentinelJedisUtil.delHSet("var_862631034775758", "Relay1"));
//		System.out.println("时间："+SentinelJedisUtil.delHSet("var_862631034775758", "RelayTime1"));
//		System.out.println("充电继电器状态："+SentinelJedisUtil.delHSet("var_862631034775758", "Relay2"));
//		System.out.println("时间:"+SentinelJedisUtil.delHSet("var_862631034775758", "RelayTime2"));
//		System.out.println("位置时间:"+SentinelJedisUtil.delHSet(	"Last_862631034775758", "PositionTime"));
		/**********************当天***************************/
//		System.out.println("dang"+SentinelJedisUtil.delHSet("var_862631034775758", "ChargingStateTime"));
//		System.out.println("dang1"+SentinelJedisUtil.delHSet("Day_862631034775758_20170123", "itChargingTime"));
//		System.out.println("开关机状态："+SentinelJedisUtil.delHSet(	"Last_862631034775758", "SwitchState"));
		/***********/
		
		System.out.println("当天开机时间："+SentinelJedisUtil.getHSet("Day_862631034775758_20170122", "iWork"));
		System.out.println("昨天累计开机机时间："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "itOpenTime"));
		System.out.println("当天停放时间："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "iParking"));
		System.out.println("当天充电次数："+SentinelJedisUtil.getHSet("Day_862631034775758_20170116", "iChargingTimes"));
		System.out.println("截止当天累计soc上升百分比："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "itSocRise"));
		 System.out.println("截止当天累计soc下降百分比："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "itSocDecline"));
		System.out.println("soc上升百分比："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "SocRise"));
		System.out.println("soc下降百分比："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "SocDecline"));
		System.out.println("当天充电时间："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "iChargingTime"));
	    System.out.println("当天充电电量："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "iChargeQuantity"));
		System.out.println("当天使用电量："+SentinelJedisUtil.getHSet("Day_862631034775758_20170123", "iConsumeQuantity"));
				
		/***********************最新**************************/
		
		System.out.println("累计开机时间："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "tWork"));
		System.out.println("累计停放时间："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "tParking"));
		System.out.println("累计充电次数："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "tChargingTimes"));
		System.out.println("电池容量："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "BatteryCapacity"));
		System.out.println("累计使用电量："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "tConsumeQuantity"));
		System.out.println("开关机状态："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "SwitchState"));
		System.out.println("实时电量："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "Electricity"));
		System.out.println("充放电状态："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "ChargingState"));
		System.out.println("电流："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "ElectricCurrent"));
		System.out.println("soc:"+SentinelJedisUtil.getHSet(	"Last_862631034775758", "Soc"));
		System.out.println("总电压："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "tVoltage"));
		System.out.println("/*************GPS_Info***********/");
		System.out.println("经度："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "Lo"));
		System.out.println("纬度："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "La"));
		System.out.println("速度："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "Speed"));
		System.out.println("方向："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "Direction"));
		System.out.println("位置时间:"+SentinelJedisUtil.getHSet(	"Last_862631034775758", "PositionTime"));
		System.out.println("/*************主继电器状态***********/");
		System.out.println("继电器状态："+SentinelJedisUtil.getHSet("var_862631034775758", "Relay1"));
		System.out.println("时间："+SentinelJedisUtil.getHSet("var_862631034775758", "RelayTime1"));
		System.out.println("充电继电器状态："+SentinelJedisUtil.getHSet("var_862631034775758", "Relay2"));
		System.out.println("时间:"+SentinelJedisUtil.getHSet("var_862631034775758", "RelayTime2"));
		
		/******************************************/ 
		
		System.out.println("开关机状态："+SentinelJedisUtil.getHSet("Last_862631034775758", "SwitchState"));
		System.out.println("开关机状时间："+SentinelJedisUtil.getHSet("Last_862631034775758", "SwitchStateTime"));
		
		
		System.out.println("总电压："+SentinelJedisUtil.getHSet("Last_862631034775758", "tVoltage"));
		System.out.println("冲放电状态:"+SentinelJedisUtil.getHSet("var_862631034775758", "ChargingState"));
		
		System.out.println("单体电压1:"+SentinelJedisUtil.getHSet("Last_862631034775758", "Voltage1"));
		System.out.println("温度1:"+SentinelJedisUtil.getHSet("Last_862631034775758", "Temp1"));
		

		/*****************************************************/
		
		System.out.println("软件版本："+SentinelJedisUtil.getHSet(	"Last_862631034775758", "SoftEdition"));
		System.out.println("硬件版本："+SentinelJedisUtil.getHSet("Last_862631034775758", "HardEdition"));
		
//		System.out.println(Math.abs(null)
//				);
	}
		// TODO Auto-generated method stub		
//		String yesterday=TimeUtil.getformatString2(new Date(new Date().getTime()-(24*60*60*1000)));
//        System.out.println(yesterday);
//        
////        System.out.println(Integer.valueOf("null"));
//        String mm="0.5";
//		if(new BigDecimal(mm).doubleValue()<0.1){
//			
//			  if(SentinelJedisUtil.getHSet("u","Soc")==null||new BigDecimal(SentinelJedisUtil.getHSet("u","Soc")).doubleValue()>0.1){
//				  
//				System.out.println("diyici baojing");	
//				
//			  } 
//			  
//			}
//		//报警解除
//		if(SentinelJedisUtil.getHSet("u","Soc")!=null){
//		if(new BigDecimal(SentinelJedisUtil.getHSet("u","Soc")).doubleValue()<0.1){
//			if(new BigDecimal(mm).doubleValue()>0.1){
//				System.out.println("jiechu");
//			}
//			
//		}
//		}
//			
////	
//	
////		
//	SentinelJedisUtil.setHSet("u","Soc",mm);
	
//	System.out.println(TimeUtil.getTimestamp(null));
//	System.out.println(SentinelJedisUtil.setHSet("z","a",null));
		
		
//		JedisPoolConfig config = new JedisPoolConfig();
//		Set<String> set=new HashSet<String>();
//		set.add("192.168.30.101:26379");
//		this.jedisSentinelPool = new JedisSentinelPool("master",set, config, 2000,"tykj66TYKJ",0) ;         
		
//	
//		Map map=new HashMap();
//		map.put("a", "m");
//		map.put("b","k");
//		
//		Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
//		  while (it.hasNext()) {
//		   Map.Entry<String, String> entry = it.next();
//		   System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
//		  }
//		String a="b";
//		System.out.println(a.equals("b"));
//		
//		String charging_report_json = "{\"RcvTime\":\"" + null + "\",\"Imei\":\"" + null
//				+ "\",\"MsgTime\":\"" + null + "\",\"RcvTime\":\"" + null + "\",\"State\":\""
//				+ null + "\",\"BeginTime\":\"" + null + "\",\"EndTime\":\"" + null + "\"}";
//		//7 days out
//		Map<String,String> map=new HashMap<>();
////		map.put("a", "dang");
////		map.put("b", "wei");
//		Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
//		  while (it.hasNext()) {
//		   Map.Entry<String, String> entry = it.next();
//		   switch (entry.getKey()) {
//			case "a":
//				System.out.println("a");
//				
//				break;
//		    case "b":
//				System.out.println("b");
//				
//				break;}
//		   System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
//		  }
//		System.out.println(TimeUtil.getformatString2(new Date(new Date().getTime()-60*24*60*1000)));
//		System.out.println(SentinelJedisUtil.expire("D" + TimeUtil.getformatString2(new Date()), 24*60*60*7));
//	String code="1100011101100110000"   ;
//	int num=32;
//		System.out.println(autoGenericCode(code,num));
//	
//	}
//	private static String autoGenericCode(String code, int num) {
//        String result = "";
//        
//        result = String.format("%0" + num + "d", Long.parseLong(code));
//
//        return result;
		  public static String BinToHex(String inStrBin)
		    {  
		       StringBuilder sb = new StringBuilder();
		       int length=inStrBin.length();
		       if(length%8!=0){
		         int num=((length/8)+1)*8;
		         inStrBin=autoGenericCode(inStrBin,num);
		       }
		       System.out.println(inStrBin);
		     for(int i=0;i<inStrBin.length();i+=4)
		        {
		            switch (subString(inStrBin,i,4))
		            {
		                case "0000":
		                    sb.append("0");
		                    break;
		                case "0001":
		                    sb.append("1");
		                    break;
		                case "0010":
		                    sb.append("2");
		                    break;
		                case "0011":
		                    sb.append("3");
		                    break;
		                case "0100":
		                    sb.append("4");
		                    break;
		                case "0101":
		                    sb.append("5");
		                    break;
		                case "0110":
		                    sb.append("6");
		                    break;
		                case "0111":
		                    sb.append("7");
		                    break;
		                case "1000":
		                    sb.append("8");
		                    break;
		                case "1001":
		                    sb.append("9");
		                    break;
		                case "1010":
		                    sb.append("A");
		                    break;
		                case "1011":
		                    sb.append("B");
		                    break;
		                case "1100":
		                    sb.append("C");
		                    break;
		                case "1101":
		                    sb.append("D");
		                    break;
		                case "1110":
		                    sb.append("E");
		                    break;
		                case "1111":
		                    sb.append("F");
		                    break;
		                default:
		                    return sb.toString();
		            }
		        }
		     
		        return sb.toString();  
		    }
		   private static String subString(String s,int startPosition,int length){
		        int Endposition=length+startPosition;
		        String ss=s.substring(startPosition, Endposition);      
		        return ss;
		      }
		  
		      private static String autoGenericCode(String code, int num) {		        
		          int length=code.length();
		          for(int i=0;i<(num-length);i++){
		        	  code="0"+code;
		          }
		          return code;
		      }
		      
		      public static void del_LAST_redis(String imei){
		    	  /********var_***********/
		    	  SentinelJedisUtil.delHSet("var_"+imei, "RelayTime1");
		    	  SentinelJedisUtil.delHSet("var_"+imei, "Relay1");
		    	  SentinelJedisUtil.delHSet("var_"+imei, "RelayTime2");
		    	  SentinelJedisUtil.delHSet("var_"+imei, "Relay2");
		    	  /********Last***********/
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "Lo");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "La");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "Speed");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "Direction");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "PositionTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tWork");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tWorkTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tParking");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tParkingTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tChargingTimes");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tChargingTimesTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "BatteryCapacity");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "BatteryCapacityTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tConsumeQuantity");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "tConsumeQuantityTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "SwitchState");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "ChargingStateTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "ElectricCurrent");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "ElectricCurrentTime");
		    	  SentinelJedisUtil.delHSet("Last_"+imei, "Electricity");
		      }
		

		
    
}
