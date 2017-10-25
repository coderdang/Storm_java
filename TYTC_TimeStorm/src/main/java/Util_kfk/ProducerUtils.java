package Util_kfk;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import Utils_Time.LogInfo;
import Utils_Time.PrintException;

public class ProducerUtils {

	private static String Cluster = "Cluster";
	private static Producer<String, String> producer;

	private ProducerUtils() {

	}
	public static void initPropsCofig() {
		// 无参数则读取代码中默认的本地ip地址。不使用集群。
		Cluster = "Single";	
		try {
			Properties props = new Properties();
			InputStream in = ProducerUtils.class.getResourceAsStream("/Config/kafka_producter.properties");
			props.load(in);
			producer = new KafkaProducer<String, String>(props);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogInfo.appendLog("kfk.err", PrintException.getStackTrace(e));
		}

	}

	static int count = 1;

	public static void produceMessage(String TOPIC, String Key, String value) {

		System.out.println("______Kafka(V0.10.x)ProducerDemo--->main--->ProducerConfig --->send......"
				+ " time  . please wait 1sec .");
		producer.send(new ProducerRecord<String, String>(TOPIC, Key, value));
		System.out.println("producerRecord: " + count++);
	}

	public static void close() {
		producer.close();
	}

}
