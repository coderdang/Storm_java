package Utils_Time;

import java.io.PrintWriter;
import java.io.StringWriter;

public class PrintException {
	
	
	 /**
	   * 获取异常的堆栈信息
	   * 
	   * @param t
	   * @return
	   */
	  public static String getStackTrace(Throwable e)
	  {
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);

	    try
	    {
	      e.printStackTrace(pw);
	      return sw.toString();
	    }
	    finally
	    {
	      pw.close();
	    }
	  }

}
