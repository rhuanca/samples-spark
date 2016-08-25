import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class Example3 {

	private static final Logger logger = Logger.getLogger(Example3.class);

	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName("Example1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
		JavaRDD<Integer> pairNumbers = numbers.filter(new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer item) throws Exception {
				return item % 2 == 0;
			}
		});
		
		pairNumbers.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer item) throws Exception {
				logger.info("item: " + item);
			}
		});
		
		sc.close();
		
	}
}
