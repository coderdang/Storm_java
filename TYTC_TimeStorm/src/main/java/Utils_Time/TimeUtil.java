package Utils_Time;


import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * 
 * @author dangwei
 *
 */
public class TimeUtil {  
//	private static SimpleDateFormat simpleDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//	private static SimpleDateFormat simpleDateFormat2 =new SimpleDateFormat("yyyyMMdd");
//	public static String getformatString(Date date) {
//		return simpleDateFormat.format(date);
//	}
//	public static String getformatString2(Date date) {
//		return simpleDateFormat2.format(date);
//	}
////	
//	public static Date getDate(String formatString) {
//		Date date = null;
//		try {
//			date = simpleDateFormat.parse(formatString);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		return date;
//	}
	
	public static String toString(Date d, String fmt) {
		if (null == d)
			return "";

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(fmt);
			return sdf.format(d);
		} catch (Exception e) {

		}
		return d.toString();
	}
	
	public static Date toDate(String d, String fmt) {
		if (null == d)
			return new Date();

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(fmt);
			return sdf.parse(d);
		} catch (Exception e) {

		}
		return new Date();
	}
	


}
