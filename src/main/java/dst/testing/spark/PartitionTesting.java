package dst.testing.spark;

// example data from udemy.com/course/apache-spark-for-java-developers


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionTesting {

    public static void main(String[] args) {

        String INPUT_FILENAME = "target/app.log"; // more data - after a while of testing

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("PartitionTesting App");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {

            // BETTER do it like this - file is read in parallel by Spark:
            JavaRDD<String> inputFileLines = sc.textFile(INPUT_FILENAME);

            System.out.println("Initial partition size: " + inputFileLines.getNumPartitions());

            JavaPairRDD<String, String> logTypeMessageRDD = inputFileLines
                    .filter(i -> i != null && i.length() > 25)
                    .mapToPair(i -> new Tuple2<>(i.substring(20, 25), i.substring(26)));

            System.out.println("Partition size after getTake(): " + inputFileLines.getNumPartitions());

            JavaPairRDD<String, Iterable<String>> groupedByErrorType = logTypeMessageRDD.groupByKey();

            System.out.println("Partition size after groupByKey(): " + groupedByErrorType.getNumPartitions());
            JavaRDD<Tuple2<String, Long>> countLogTypes = groupedByErrorType.map((t) -> {
                AtomicLong i = new AtomicLong();
                t._2().forEach(v -> i.incrementAndGet());
                return new Tuple2<>(t._1, i.longValue());
            });

            // switch to PairRDD to use sortByKey
            countLogTypes.mapToPair(t -> new Tuple2<>(t._2, t._1))
                    .sortByKey(false) // descending
                    .mapToPair(t -> new Tuple2<>(t._2, t._1));

            countLogTypes.take(10).forEach(i-> System.out.println(" " + i._1 + " = " + i._2));

            // Action
            System.out.println( "count=" +  countLogTypes.count() );


            // Remove (comment) after testing:
            System.out.println("\n===== DEBUG ONLY ! ======\n\tOpen Spark-Jobs page at http://localhost:4040 to analyze execution performande - Press Enter when finished\n=========================\n");
            new Scanner(System.in).nextLine();
        }

    }
}

