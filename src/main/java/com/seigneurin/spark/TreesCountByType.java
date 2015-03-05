package com.seigneurin.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TreesCountByType {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("trees");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> rdd = sc
                .textFile("/Users/aseigneurin/dev/spark-sandbox/data/arbresalignementparis2010.csv", 2)
                .filter(line -> !line.startsWith("geom"))
                .map(line -> line.split(";"))
                .mapToPair(fields -> new Tuple2<String, Integer>(fields[4], 1))
                .reduceByKey((x, y) -> x + y)
                .coalesce(1)
                .sortByKey();

        rdd.foreach(t -> System.out.println(t._1 + " : " + t._2));

        System.out.println(rdd.count());

        sc.close();
    }

}
