package storm.project.TYTC_TimeStorm;

import java.util.Date;

import Utils_Time.LogInfo;
import Utils_Time.TimeUtil;

public class test1 extends Thread { 
	static int i=0;
	public void run() {
		// TODO Auto-generated method stub
		System.out.println(i+":"+timeSubtract(i++,0));
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
for (int i = 0; i < 10; i++) {
	new test1().start();

}	
		
	}
	
	public synchronized int timeSubtract(int a,int b){
     int  c=a+b;
	return c;

	}
	
	

}
