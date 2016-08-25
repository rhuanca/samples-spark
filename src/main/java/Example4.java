import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class Example4 {

	private static final Logger logger = Logger.getLogger(Example4.class);
	private static final int MAXITEMS = 10;

	private static class ThingMessage implements Serializable {

		private static final long serialVersionUID = 1L;

		private String serialNumber;
		private long timeStamp;
		private Map<String, String> map;

		public ThingMessage(String serialNumber, long timeStamp) {
			this.serialNumber = serialNumber;
			this.timeStamp = timeStamp;
			this.map = new HashMap<>();
		}

		public String getSerialNumber() {
			return serialNumber;
		}

		public long getTimeStamp() {
			return timeStamp;
		}

		public void put(String key, String value) {
			map.put(key, value);
		}

		public String get(String key) {
			return map.get(key);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("ThingMessage [");
			sb.append("serialNumber:").append(serialNumber).append(",");
			sb.append("timeStamp:").append(serialNumber);
			if (!map.isEmpty()) {
				sb.append(",");
				for (Iterator<String> iterator = map.keySet()
						.iterator(); iterator.hasNext();) {
					String key = (String) iterator.next();
					sb.append(key).append(":").append(map.get(key));
					if (iterator.hasNext()) {
						sb.append(",");
					}
				}
			}
			sb.append("]");
			return sb.toString();
		}
	}

	public static void main(String args[]) {

		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName(Example4.class.getName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		ArrayList<ThingMessage> list = new ArrayList<>(MAXITEMS);

		for (int i = 0; i < MAXITEMS; i++) {
			list.add(new ThingMessage(String.format("%10d", (i + 1)), (i + 1)));
		}

		JavaRDD<ThingMessage> things = sc.parallelize(list);

		JavaRDD<ThingMessage> thingsWithZones = things.map(
				new Function<Example4.ThingMessage, Example4.ThingMessage>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Example4.ThingMessage call(ThingMessage tm)
							throws Exception {
						if (tm.getTimeStamp() % 2 == 0) {
							tm.put("zone", "parking");
						} else {
							tm.put("zone", "");
						}
						return tm;
					}
				});

		thingsWithZones.foreach(new VoidFunction<Example4.ThingMessage>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(ThingMessage thing) throws Exception {
				logger.info(thing);
			}
		});

		sc.close();
	}
}
