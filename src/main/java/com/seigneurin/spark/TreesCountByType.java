package com.seigneurin.spark;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TreesCountByType {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "trees");

        sc.textFile("/Users/aseigneurin/dev/spark-sandbox/data/arbresalignementparis2010.csv")
                .filter(line -> !line.startsWith("geom"))
                .map(line -> line.split(";"))
                .mapToPair(fields -> new Tuple2<String, Integer>(fields[4], 1))
                .reduceByKey((x, y) -> x + y)
                .sortByKey()
                .foreach(t -> System.out.println(t._1 + " : " + t._2));

        sc.close();
    }

}
