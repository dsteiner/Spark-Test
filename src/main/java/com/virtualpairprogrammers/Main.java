package com.virtualpairprogrammers;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    public static void main(String[] args) {
        List<String> logEntries = readLogFile("target/app.log");

        //Logger.getLogger("org.apache").setLevel(Level.WARN);
        //Logger.getLogger("org.apache").setLevel(Level.WARN);
        //Logger.getRootLogger().setLevel(Level.WARN);


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark application");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> logLines = sc.parallelize(logEntries);



            // inter immediate count
            Long count = logLines.map(line -> 1L).reduce(Long::sum);
            System.out.println("count: " + count);



            // Group by key :=  Chrashy / performance problem!!! Avoid it!
            /*
                    JavaPairRDD<String, String> logTuples = logLines.mapToPair(line -> new Tuple2<>(line.substring(20, 25), line));
                    JavaPairRDD<String, Iterable<String>> groupedByKeyRDD = logTuples.groupByKey();
                    JavaRDD<Tuple2<String, Long>> countLogTypes = groupedByKeyRDD.map((t) -> { // Better use GUAVA: Iterables.size(...)
                        AtomicLong i = new AtomicLong();
                        t._2().forEach(v -> i.incrementAndGet());
                        return new Tuple2<>(t._1, i.longValue());
                    });
                    countLogTypes.foreach(t -> System.out.println(t._1() + " = " + t._2()));
             */

            // logTuples.foreach(System.out::println); -> Serialization error when running on multiple CPUS in a cluster
            // Lösung:  collect all together in on CPU -> List<String> -> forEach statt foreach
            //logTuples.map(t -> t._1).collect().forEach(System.out::println);

            // Better way: reduceByKey
            /*
                    logLines.mapToPair(line -> new Tuple2<>(line.substring(20, 25), 1L))
                    .reduceByKey(Long::sum)
                    .foreach(t -> System.out.println(t._1() + " = " + t._2()));
            */
        }


    }

    private static List<String> readLogFile(String fileName) {
        List<String> lines = new ArrayList<>();
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

}