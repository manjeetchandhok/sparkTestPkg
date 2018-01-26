package sparkTestPkg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Java7WordCount {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		JavaSparkContext sc = new JavaSparkContext(conf);

		wordCount();

	}

	public static void wordCount() throws Exception {
		String inputFile = "E:\\test2.txt";
		String outputFile = "E:\\test3\\test2.txt";
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		// Split line into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				System.out.println();
				Iterator<String> test = Arrays.asList(x.split(" ")).iterator();
				return test;
			}
		});

		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
	}

}
