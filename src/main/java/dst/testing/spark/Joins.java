package dst.testing.spark;

// example data from udemy.com/course/apache-spark-for-java-developers


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.List;

public class Joins {

    List<Tuple2<String, String>> idPlusNameList = List.of(
            new Tuple2<>("1", "A"),
            new Tuple2<>("2", "B"),
            new Tuple2<>("3", "C")
    );
    List<Tuple2<String, Integer>> idPlusBalanceList = List.of(
            new Tuple2<>("1", 1000),
            new Tuple2<>("2", 2000),
            new Tuple2<>("4", 3000 ),
            new Tuple2<>("5", 4000 ));


    public static void main(String[] args) {

        Joins joins = new Joins();

        joins.innerJoin();
        joins.leftOuterJoin();
        joins.rightOuterJoin();
        joins.fullJoin();



    }

    private void innerJoin() {
        System.out.println("\n(Inner) Join");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("(Full) Join Example App");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<String, String> idNamesRDD = sc.parallelizePairs(idPlusNameList);
            JavaPairRDD<String, Integer> idBalanceRDD = sc.parallelizePairs(idPlusBalanceList);
            JavaPairRDD<String, Tuple2<String, Integer>> join = idNamesRDD.join(idBalanceRDD);

            // avoid serialization error on PrintStream - use collect()
            join.collect().forEach(System.out::println);
        }
    }
    private void leftOuterJoin() {
        System.out.println("\n(Left) Outer Join");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Left (outer) Join Example App");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<String, String> idNamesRDD = sc.parallelizePairs(idPlusNameList);
            JavaPairRDD<String, Integer> idBalanceRDD = sc.parallelizePairs(idPlusBalanceList);
            JavaPairRDD<String, Tuple2<String, Optional<Integer>>> join = idNamesRDD.leftOuterJoin(idBalanceRDD);


            // avoid serialization error on PrintStream - use collect()
            join.collect().forEach( i -> System.out.println("" + i._1 + " " + i._2._1 + " " + i._2._2.orElse(0) ));
        }

    }
    private void rightOuterJoin() {
        System.out.println("\nRight (outer) Join");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Right (outer) Join Example App");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<String, String> idNamesRDD = sc.parallelizePairs(idPlusNameList);
            JavaPairRDD<String, Integer> idBalanceRDD = sc.parallelizePairs(idPlusBalanceList);
            JavaPairRDD<String, Tuple2<Optional<String>, Integer>> join = idNamesRDD.rightOuterJoin(idBalanceRDD);


            // avoid serialization error on PrintStream - use collect()
            join.collect().forEach( System.out::println);
        }
    }

    private void fullJoin() {
        System.out.println("\nCartesian-Join, Full-Join or Cross-Join");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Full Join Example App");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<String, String> idNamesRDD = sc.parallelizePairs(idPlusNameList);
            JavaPairRDD<String, Integer> idBalanceRDD = sc.parallelizePairs(idPlusBalanceList);
            JavaPairRDD<Tuple2<String, String>, Tuple2<String, Integer>> join = idNamesRDD.cartesian(idBalanceRDD);


            // avoid serialization error on PrintStream - use collect()
            join.collect().forEach( System.out::println);
        }
    }


}

