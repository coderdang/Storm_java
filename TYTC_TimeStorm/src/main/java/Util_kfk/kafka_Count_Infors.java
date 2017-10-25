package Util_kfk;

import java.util.Arrays;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.storm.utils.Utils;
import Utils_Time.LogInfo;
import storm.project.TYTC_TimeStorm.Topology;


public class kafka_Count_Infors implements Runnable{
	private Properties props;
	private KafkaConsumer<String, String> consumer;
	public static Integer count = 0;
	public kafka_Count_Infors() {		
		Topology.logType = 1;
		// 配置kfk消费者参数
		props = new Properties();
		try {
			props.load(this.getClass().getResourceAsStream("/Config/kafka_consumer_test.properties"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("TYTC_Source"));// topic can more
	}
	//线程方法
	public void run() {
		for(;;){
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {		
				String value = record.value();
				if (value != null) {
					count++;
					System.out.println(count);
				}
			}
		}
		
	}
	//普通方法体，由main线程驱动，分配内存
	public void  B() {		
		for(;;){
			String count1=kafka_Count_Infors.count.toString();			
			LogInfo.appendLog("test_count",count1 );
			Utils.sleep(10000);
		}
	}	
	
	
	public void check_Date_FromKfk() {
		for(;;){
			ConsumerRecords<String, String> records = consumer.poll(10);
			for (ConsumerRecord<String, String> record : records) {		
				String value = record.value();
				if (value.contains("800000000000005")||value.contains("800000000000004")||value.contains("800000000000003")||value.contains("800000000000002")||value.contains("800000000000001")) {					
					System.out.println(value);
				}
			}
		}
		
	}

	public static void main(String[] args) {
		//   
		kafka_Count_Infors kfk_count=new kafka_Count_Infors();	
		Thread thread =new Thread(kfk_count);
		thread.start();
		kfk_count.B();
	}
}


	
			


	



