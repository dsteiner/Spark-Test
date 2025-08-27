package dst.testing.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // when using "sc.textFile(...)" on Windows you might need to set the hadoop home dir and have winutils.exe available in <HADOOP_HOME>\bin
        // or uncomment and replace with your path to hadoop home dir if needed - e.g. on Windows:
        /*
        System.setProperty("hadoop.home.dir", "C:\\<Yout-Path-To-Hadoop-WinUtils>"); // OR HADOOP_HOME
        */
        // and put <HADOOP_HOME>\bin\winutils.exe into your PATH additionally to avoid a WARN log messages like
        // "... FileNotFoundException.... winutils.exe ... "  or "NativeCodeLoader - Unable to load native-hadoop library ...."

        String INPUT_FILENAME = "src/main/java/dst/testing/spark/Main.java";
        // more data - after a while of testing - uncomment this:
        //INPUT_FILENAME = "src/main/java/resources/log4j2.properties";

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark application");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            // read file - COULD do it like this - but file is read into memory first:
            /*
             List<String> logEntries = readLinesOfFile("src/main/java/dst/testing/spark/Main.java");
             JavaRDD<String> logLines = sc.parallelize(logEntries);
             */


            // BETTER do it like this - file is read in parallel by Spark:
            sc.textFile(INPUT_FILENAME);
            JavaRDD<String> inputFileLines = sc.textFile(INPUT_FILENAME);


            // inter immediate count
            Long count = inputFileLines.map(line -> 1L).reduce(Long::sum);
            System.out.println("lines count: " + count);

            // groupByKey
            // Group by key :=  crashy and/or performance problem!!! avoid it!
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

            // HINT:
            // logTuples.foreach(System.out::println); -> Serialization error might occur when running on multiple CPUs in a cluster
            // Solution:  "collect()" all together to one CPU and get a Java-List<String> instead -> forEach (Java) vs. foreach (Spark)
            // e.g.: logTuples.map(t -> t._1).collect().forEach(System.out::println);

            // Better way: reduceByKey
            /*
                    logLines.mapToPair(line -> new Tuple2<>(line.substring(20, 25), 1L))
                    .reduceByKey(Long::sum)
                    .foreach(t -> System.out.println(t._1() + " = " + t._2()));
            */

            // FlatMap
            /*
            logLines.flatMap(line -> List.of(line.split(" ")).iterator())
                    .mapToPair(word -> new scala.Tuple2<>(word, 1L))
                    .reduceByKey(Long::sum)
                    .foreach(t -> System.out.println(t._1() + " = " + t._2()));
             */
        }


    }

    private static List<String> readLinesOfFile(String fileName) {
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