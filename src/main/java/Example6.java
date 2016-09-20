import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class Example6 {

	public static void main(String args[]) {
		// Create spark configuration
		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkStreamingMqttTest").setMaster("local[3]");

		// spark streaming context with a 10 second batch size
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(10));

		// Define MQTT url and topic
		String brokerUrl = "tcp://localhost:1883";
		String topic = "/data";

		// collect MQTT data using streaming context and MQTTUtils library

		JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(ssc,
				brokerUrl, topic);

		stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> t) throws Exception {
				System.out.println("Count: " + t.count());
				List<String> collect = t.collect();
				for (String string : collect) {
					System.out.println("Msg = " + string);
				}
			}
		});
		ssc.start();

		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			System.err.println(e);
		}
	}
}
