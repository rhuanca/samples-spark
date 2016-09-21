import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import scala.collection.Iterable;
import scala.collection.Map;

public class Example7 {
	private static Logger logger = Logger.getLogger(Example7.class);
	
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
			Map<Object, RDD<?>> persistentRDDs = t.context().getPersistentRDDs();
			scala.collection.Iterator<Object> keys = persistentRDDs.keys().iterator();
			while(keys.hasNext()) {
				Object key = keys.next();
				logger.info(">>> key = " + key);
				logger.info(">>> value = " + persistentRDDs.get(key) );
				logger.info(">>> value.getClass() = " + persistentRDDs.get(key).getClass() );
				
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
				.setAppName("SparkStreamingMqttTest").setMaster("local[1]");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(10));
		
	    ExecutorService executorService = Executors.newFixedThreadPool(3);
	    

	    executorService.submit(new Runnable() {
			
			@Override
			public void run() {
				String brokerUrl = "tcp://localhost:1883";
				String topic = "/data";

				JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(ssc,
						brokerUrl, topic);

				Processor processor = new Processor("val");
				
				stream.foreachRDD(processor);
	
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

				JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(ssc,
						brokerUrl, topic);

				ConfigUpdater configUpdater = new ConfigUpdater();
				
				stream.foreachRDD(configUpdater);
				
				

				try {
					ssc.awaitTermination();
				} catch (InterruptedException e) {
					System.err.println(e);
				}				
			}
	    });
	    
	    ssc.start();
	}
}
