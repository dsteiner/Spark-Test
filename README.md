# Project Setup for testing Spark (3.5.6) with Java

This project is to initially setup Apache Spark (version 3.5.6) programming with Java 
(11 - NOT above - must use Spark 4.x.y. for this). 

It includes the necessary dependencies and logging configuration to get started with Spark development in Java and Maven

## Prerequisites
* Java 11 (NOT above - must use Spark 4.x.y. for this)
  * add VM-Options: 

        --add-exports java.base/sun.nio.ch=ALL-UNNAMED
        --add-opens java.base/java.nio=ALL-UNNAMED 
        --add-opens java.base/java.net=ALL-UNNAMED 
        --add-opens java.base/java.lang=ALL-UNNAMED 
        --add-opens java.base/java.util=ALL-UNNAMED 
        --add-opens java.base/java.util.concurrent=ALL-UNNAMED
        --add-exports java.base/sun.security.action=ALL-UNNAMED
        --add-opens java.base/java.io=ALL-UNNAMED
        

## Example Code

* Read https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html
* Start programming with [FirstSteps.java](src/main/java/FirstSteps.java) and read/test comments
* Work your way through  https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html
* Hands Om - Step by Step:

### JavaSparkRDD 
1. [FirstStep.java](src/main/java/dst/testing/spark/FirstSteps.java)

2. [FindImportantWordExample.java](src/main/java/dst/testing/spark/FindImportantWordExample.java)
   
3. [Joins.java](src/main/java/dst/testing/spark/Joins.java)

4. [BigDataExcercise.java](src/main/java/dst/testing/spark/BigDataExcercise.java)

### SparkSQL

1. [SparkSQLFirstSteps.java](src/main/java/dst/testing/spark/SparkSQLFirstSteps.java)
2. [SparkSQLQueriesOnMockData](src/main/java/dst/testing/spark/SparkSQLQueriesOnMockData.java)

## Performance Tuning

### Open Spark Jobs Web UI

Add following just before closing our Spark Session:

```java

    System.out.println("\n===== DEBUG ONLY ! ======\n\tOpen Spark-Jobs page at http://localhost:4040 to analyze execution performande - Press Enter when finished\n=========================\n");
    new Scanner(System.in).nextLine();

    // Alternatively, you can put an Breakpoint before the __SparkContext__is "close()"ed and DEBUG your App ....
```

### Wide Transformations

When using wide transformations (like `reduceByKey`, `groupByKey`, `sortByKey`, `join` etc.), Spark will perform a shuffle operation. This involves redistributing data across the cluster, which can be expensive in terms of time and resources.

To optimize wide transformations, consider the following strategies:

1. **Reduce Data Before Shuffle**: Use `map` or `filter` transformations before wide transformations to reduce the amount of data that needs to be shuffled.
2. **Use `reduceByKey` Instead of `groupByKey`**: When aggregating data, prefer `reduceByKey` as it combines values locally before shuffling, reducing the amount of data transferred across the network.

### Salt the Key
groupByKey() kann zu OutOfMemory führen, wenn ein Key sehr viele Values auf eine Partition verteilt und andere Partitions "leer" sind...
...muss vermident werdne - letzte Möglichkeit ist "Salt the Key" vorher - wenn alles andere nicht hilft  

When dealing with skewed data, salting the key can help distribute the data more evenly across partitions. This involves adding a random prefix or suffix to the keys before performing wide transformations.

### Caching and Persistence