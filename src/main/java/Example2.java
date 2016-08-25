import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Example2 {

	private static final Logger logger = Logger.getLogger(Example2.class);

	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName("Example1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> numbers = sc.textFile("numbers.txt");

		logger.info("first:" + numbers.first());

		JavaRDD<String> filtered = numbers
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String arg0) throws Exception {
						return Integer.parseInt(arg0) % 2 == 0;
					}
				});

		logger.info("filtered.count: " + filtered.count());
		logger.info("filtered: " + filtered);
		
		sc.close();
	}
}
