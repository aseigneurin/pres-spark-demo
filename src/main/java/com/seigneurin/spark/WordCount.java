package com.seigneurin.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("wikipedia-mapreduce-by-key")
                .setMaster("local[16]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("src/main/java/com/seigneurin/spark/*")
                .flatMap(line -> Arrays.asList(line.split("\\W")))
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((x, y) -> x + y)
                .coalesce(1)
                .sortByKey()
                .foreach(t -> System.out.println(t._1 + " = " + t._2));
    }
}
