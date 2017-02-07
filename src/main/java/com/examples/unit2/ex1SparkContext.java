package com.examples.unit2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Pankaj.Chandankar on 20/01/2017.
 */
public class ex1SparkContext {
    public static void main(String args[]) {

        /* Create a Java Spark Context */
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyApp");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            System.out.println("Spark Application : " + sc.appName());
            System.out.println("Spark Application : " + sc.getSparkHome());
        } catch (Exception e) {
            System.out.println("Spark Exception : ");
            e.printStackTrace();
        }
    }
}
