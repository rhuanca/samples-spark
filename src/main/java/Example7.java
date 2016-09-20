import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class Example7 {
	
	private static class Appender implements VoidFunction<JavaRDD<String>> {

		private String string;
		
		public Appender(String string) {
			this.string = string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public void call(JavaRDD<String> t) throws Exception {
			List<String> messages = t.collect();
			for (String msg : messages) {
				System.out.println(string + "-" + msg);
			}
		}
	}

	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkStreamingMqttTest").setMaster("local[3]");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(10));

		String brokerUrl = "tcp://localhost:1883";
		String topic = "/data";

		JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(ssc,
				brokerUrl, topic);

		Appender appender = new Appender("val");
		
		stream.foreachRDD(appender);
		
		ssc.start();

		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			System.err.println(e);
		}
	}
}
