import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import scala.collection.Map;

public class Example8 {
	private static Logger logger = Logger.getLogger(Example8.class);

	public static class Operator {
		public String operator;
	}

	public static class Processor implements VoidFunction<JavaRDD<String>> {
		private static final long serialVersionUID = 1L;

		private String string;

		public Processor(String string) {
			this.string = string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public void call(JavaRDD<String> t) throws Exception {
			List<String> messages = t.collect();
			Map<Object, RDD<?>> persistentRDDs = t.context()
					.getPersistentRDDs();
			scala.collection.Iterator<Object> keys = persistentRDDs.keys()
					.iterator();
			while (keys.hasNext()) {
				Object key = keys.next();
				logger.info(">>> key = " + key);
				logger.info(">>> value = " + persistentRDDs.get(key));
				logger.info(">>> value.getClass() = "
						+ persistentRDDs.get(key).getClass());

			}
		}
	}

	public static class ConfigUpdater implements VoidFunction<JavaRDD<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void call(JavaRDD<String> t) throws Exception {
			logger.info(">>> received config message");
			t.persist(StorageLevel.MEMORY_AND_DISK());

		}
	}

	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkStreamingMqttTest").setMaster("local[2]");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(5));
	

		String brokerUrl = "tcp://localhost:1883";
		String dataTopic = "/data";
		String configTopic = "/config";

		JavaReceiverInputDStream<String> data = MQTTUtils.createStream(ssc, brokerUrl, dataTopic);
		JavaReceiverInputDStream<String> config = MQTTUtils.createStream(ssc, brokerUrl, configTopic);
		
		List<JavaDStream<String>> streams = new ArrayList<>(2);
		streams.add(data);
		streams.add(config);
		
		JavaDStream<String> input = ssc.union(data, streams.subList(1, streams.size()-1));
		
		input.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> t) throws Exception {
				logger.info(">>>>");
				logger.info(">>>> t.collect() = " + t.collect());
			}
		});

		input.print();
		
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
