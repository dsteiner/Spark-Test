package dst.testing.spark;

// example based on udemy.com/course/apache-spark-for-java-developers


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FindImportantWordExample {


    public static void main(String[] args) {

        String INPUT_FILENAME = "src/main/resources/subtitles/input.txt";

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Find Important Word Example App");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {

            // BETTER do it like this - file is read in parallel by Spark:
            JavaRDD<String> inputFileLines = sc.textFile(INPUT_FILENAME);

            JavaRDD<String> sorted = inputFileLines
                    .map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase()) // remove punctuation, to lower case
                    .flatMap(s -> List.of(s.split("\\s+")).iterator()) // split by whitespace - one WORD per entry
                    .filter(line -> !line.trim().isEmpty()) // remove empty lines
                    .filter(BoringWordsHolder::isNotBoring) // remove boring words
                    // remove double words - distinct():
                    .mapToPair(word -> new Tuple2<>(word, 1L)) // map to (word, 1)
                    .reduceByKey(Long::sum) // count occurrences
                    // now we have (word, count) pairs
                    // sort by count descending - so swap to (count, word) first cause there is only a sortBy on key
                    .map(t -> new Tuple2<>(t._2, t._1)) // swap to (count, word)
                    .sortBy(t -> -t._1, true, 1) // sort by count descending
                    .map(t -> t._2 + " (" + t._1 + ")");// switch back to (word, count)

            //System.out.println("number of partitions: " + sorted.getNumPartitions()); // default is number of CPU cores - 2 on local[*]

            // DO NOT ! uses coalesce(1) - which shuffles all data to ONE worker node
            //  it forces all data to one CPU - bad for performance and might cause out-of-memory

            // When to use coalesce(...) ?
            // use it if you have MANY Partitions and after already reducing the data is small (no memory issue)
            // -> reduces the number of partitions to avoid expensive shuffling
                    /*
                    .coalesce(1)
                     */

            // .collect() - brings all data to the driver - might cause out-of-memory
            // use it only for testing with small data sets!
            // -> BETTER write to a HDFS or local file system

            List<String> correctSortedResultFromTake = sorted.take(10);// returns the correct first 10 entries - but returns a List<String> to the driver
            correctSortedResultFromTake.forEach(System.out::println);

            // TAKE CARE!
            // "send" "foreach" lambda to ALL Workers to each partition  (in PARALLEL -> THREAD)
            // -> sorted output is correct for each partition, so the output you see might be "mixed"!
/*
            sorted.foreach(s -> System.out.println(s)); // System.out::println is not serializable
*/
        }

    }

    private static class BoringWordsHolder {
        private static Set<String> borings = new HashSet<>();

        static {
            try {
                borings.addAll(Files.readAllLines(Paths.get("src/main/resources/subtitles/boringwords.txt"), StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static boolean isNotBoring(String word) {
            return !borings.contains(word);
        }
    }
}
