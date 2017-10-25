package Utils_Time;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Scanner;

import storm.project.TYTC_TimeStorm.Topology;

public class LogInfo {

	synchronized public static void appendLog(String logFileName, String newLog) {		
		switch(Topology.logType)
		{
			case 1:
				appendLog1("log\\"+ logFileName +".log",newLog);//本地路径				
				break;
			case 2:
				appendLog1("/TYTC_time_logs/" +logFileName + ".log",newLog);//本地路径
				break;
		}
	}
	
	/**生成本地根目录log文件	 
	 * @param logFileName
	 *            日志文件名
	 * @param newLog
	 *            日志内容+ [yyyy-MM-dd hh:mm:ss.SSS]
	 */
	synchronized private static void appendLog1(String path, String newLog) {
		PrintWriter pw = null;
		File log = new File(path);//服务器路径		
		try {
			if (!log.exists())// 如果文件不存在,则新建.
			{
				File parentDir = new File(log.getParent());
				
				if (!parentDir.exists())// 如果所在目录不存在,则新建.
				{
					parentDir.mkdirs();
				}
				log.createNewFile();
			}						
			pw = new PrintWriter(new FileWriter(log,true), true);
			pw.println("[" + getCurrentDate() + "]"+newLog);// 写入新日志.			
			pw.close();

		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	/**当前时间		
	 * @return
	 */
	private static String getCurrentDate() {
		SimpleDateFormat sm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sm.format(new Date());
	}
}
