package dst.testing.spark;

// example data from udemy.com/course/apache-spark-for-java-developers


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class SparkSQLFirstSteps {

    public static void main(String[] args) {

        String INPUT_FILENAME = "src/main/resources/exams/students.csv"; // more data - after a while of testing

        try (SparkSession spark = SparkSession.builder().appName("Spark SQL First Steps").master("local[*]").config("spark.sql.warehouse.dir", "file:///c:/temp/").getOrCreate()) {


            Dataset<Row> studentsDataset = spark.read().option("header", true).csv(INPUT_FILENAME);
            System.out.println("total number of rows: " + studentsDataset.count());
/*
            studentsDataset.show(); // shows first 20 rows


*/
            studentsDataset.printSchema(); // shows the schema of the data
/*
            Row row = studentsDataset.first();
            String subject = row.get(2).toString();
            String year = row.getAs("year").toString();
*/

/*
            Dataset<Row> filtered = studentsDataset.filter("subject = 'Modern Art' AND year = '2014'");
            filtered.show();
*/
/*
            Dataset<Row> filtered = studentsDataset.filter((FilterFunction<Row>) row ->
                    "Modern Art".equals(row.getAs("subject")) && "2014".equals(row.getAs("year"))
            );
*/

            /*
            Dataset<Row> filtered = studentsDataset.filter(col("subject").equalTo("Modern Art").and(col("year").equalTo("2014")));
*/

            // SparkSQL is just like real SQL
            studentsDataset.createOrReplaceTempView("myStudentsTtable");
            //Dataset<Row> filtered = spark.sql("select max(score) from myStudentsTtable Where subject = 'Modern Art' AND year = '2014'");
            Dataset<Row> filtered = spark.sql("select distinct year from myStudentsTtable");
            System.out.println("number of filtered rows: " + filtered.count());
            filtered.show();


            // uncomment for testing:
            //System.out.println("\n===== DEBUG ONLY ! ======\n\tOpen Spark-Jobs page at http://localhost:4040 to analyze execution performande - Press Enter when finished\n=========================\n");
            //new Scanner(System.in).nextLine();
        }

    }
}

