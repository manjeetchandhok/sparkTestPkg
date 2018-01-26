package sparkTestPkg;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class Java7WordCount {
	
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//test(sc);
		wordCount();
		
		
	}
	
	public static void wordCount_1() throws Exception {
		String inputFile = "E:\\test2.txt";
		String outputFile = "E:\\test3\\test2.txt";
		SparkConf conf= new SparkConf().setMaster("local").setAppName("Test");
		JavaSparkContext jsc= new JavaSparkContext(conf);
		
		Dataset<Row> df = conf.read().json("examples/src/main/resources/people.json");
		JavaRDD<String>input=jsc.textFile(inputFile);
		
		SQLContext sqlContext =  new SQLContext(jsc);
		Dataset<Row> urlsDF = sqlContext.createDataFrame(input, Url.class);
		
		
		
		JavaRDD<String> words=input.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		
		
		words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple3< String, Integer> call(String t) throws Exception {
				return new Tuple2(t,1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).saveAsTextFile("E:\\test");
		
	}
	
	public static void wordCount() throws Exception {
		String inputFile = "E:\\test2.txt";
		String outputFile = "E:\\test3\\test2.txt";
		// Create a Java Spark Context.
		//SparkConf conf = new SparkConf().setAppName("wordCount");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		input.cache();
		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				System.out.println();
				 Iterator<String> test = Arrays.asList(x.split(" ")).iterator();
				return test; 
			}
		});
		
		
		
		sc.getPersistentRDDs();
		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() { //kv tupper bana deya
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
	
public static void test(JavaSparkContext sc) {
	List<String> l1= new ArrayList<String>();
	l1.add("dog");
	l1.add("dog1");
	l1.add("dog2");
	l1.add("dog3");
	l1.add("dog4");
	JavaRDD<String> input =sc.parallelize(l1,5);// no of partion of executer
	JavaRDD<String> outPut= input.map(new Function<String, String>() {

		@Override
		public String call(String v1) throws Exception {
			String result = v1.trim().toUpperCase();
			return result;
		}
	});
	
	
System.out.println(outPut.collect());
	
}



}

//checkpoint
