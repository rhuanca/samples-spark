import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Example5 {

	private static final Logger logger = Logger.getLogger(Example5.class);

	public static void main(String args[]) {

		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName(Example5.class.getName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("pairs.txt");

		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String x) {
				return new Tuple2<String, String>(x.split(" ")[0], x);
			}
		};

		JavaPairRDD<String, String> pairs = lines.mapToPair(keyData);

		pairs.foreach(new VoidFunction<Tuple2<String, String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				logger.info(t._1 + ": " + t._2);
			}
		});

		sc.close();
	}
}
