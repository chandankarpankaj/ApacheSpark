package com.examples.unit2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import static java.lang.System.getProperties;

/**
 * Created by Pankaj.Chandankar on 07/02/2017.
 */
public class ex3MapReduce {

    static String inputFile = "E:\\apache-spark\\workspace\\ApacheSpark\\resources\\unit2\\word_count_test.txt";

    public static void main(String args[]) {

        // Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyApp");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Properties sysProp = getProperties();

        System.out.println("Java : " + sysProp.toString());
        System.out.println("Spark Application : " + sc.appName());
        System.out.println("Spark Home : " + sc.getSparkHome());

        // Load our input data
        JavaRDD<String> input = sc.textFile(inputFile);

        // Split up into words.
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }
                });

        // Transform into pairs and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        // If you want to view the content of a RDD, one way is to use collect()
        System.out.println("Spark RDD Data Print 1 : ");
        counts.collect().forEach(line -> System.out.println(line));

        // That's not a good idea, though, when the RDD has billions of lines. Use take() to take just a few to print out.
        System.out.println("Spark RDD Data Print 2 : ");
        counts.take(10).forEach(line -> System.out.println(line));

        // Save the RDD back out to a text file
        System.out.println("Spark RDD Data Print 3 : ");
        counts.saveAsTextFile("MyApp_counts.txt");

        System.out.println("Spark RDD Data Print 4 : " + counts.toString());
    }
}
