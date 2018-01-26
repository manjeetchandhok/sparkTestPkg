package sparkTestPkg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class MapExample {

	public static void main(String arr[]) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple map example");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> input = sc.textFile("E:\\test.txt");

		JavaRDD<String> mapFile = input.map(new Function<String, String>() {

			@Override
			public String call(String v1) throws Exception {

				return v1;
			}
		}).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String v1) throws Exception {

				return v1 != null && v1.contains("error");
			}
		});

		mapFile.foreach(x -> System.out.println(x));

	}

}
