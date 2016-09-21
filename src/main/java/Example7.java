import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class Example7 {
	private static Logger logger = Logger.getLogger(Example7.class);
	
//	private static class Config implements Serializable{
//		public String preffix;
//
//		public Config(String preffix) {
//			this.preffix = preffix;
//		}
//		
//	}
	
	public static class Processor implements VoidFunction<JavaRDD<String>> {
		private static final long serialVersionUID = 1L;

//		private Config config;
		
		@Override
		public void call(JavaRDD<String> t) throws Exception {
			logger.info(">>> data-t = " + t.count());
			t.foreach(new VoidFunction<String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void call(String t) throws Exception {
					logger.info(">>> data = " + t);
				}
			});
		}
	}
	
	public static class ConfigUpdater implements VoidFunction<JavaRDD<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void call(JavaRDD<String> t) throws Exception {
			logger.info(">>> received config message");
			logger.info(">>> t.count() = " + t.count());
			
//			System.out.println(">>> t.collect() = " + t.collect());
			t.foreach(new VoidFunction<String>() {
				
				@Override
				public void call(String t) throws Exception {
					logger.error(">>>> t = " + t);
				}
			});
		}
	}

	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkStreamingMqttTest").setMaster("local[3]");

		// spark streaming context with a 10 second batch size
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(5));
		
	    ExecutorService executorService = Executors.newFixedThreadPool(2);
	    
	    executorService.submit(new Runnable() {
			@Override
			public void run() {
				String brokerUrl = "tcp://localhost:1883";
				String topic = "/data";

				JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(ssc,
						brokerUrl, topic);

				
				stream.foreachRDD(new Processor());

				ssc.start();
				try {
					ssc.awaitTermination();
				} catch (InterruptedException e) {
					System.err.println(e);
				}
			}
		});
	    
		executorService.submit(new Runnable() {
			@Override
			public void run() {
				String brokerUrl = "tcp://localhost:1883";
				String topic = "/config";

				JavaReceiverInputDStream<String> stream = MQTTUtils
						.createStream(ssc, brokerUrl, topic);

				ConfigUpdater configUpdater = new ConfigUpdater();
				
				stream.foreachRDD(configUpdater);
			}
		});
		

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			logger.error(e1);
		}
		
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			System.err.println(e);
		}
	}
}
