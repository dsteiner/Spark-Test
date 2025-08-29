package dst.testing.spark;

// example data from udemy.com/course/apache-spark-for-java-developers


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSQLMockTesting {

    public static void main(String[] args) {

        String INPUT_FILENAME = "src/main/resources/exams/students.csv"; // more data - after a while of testing

        try (SparkSession spark = SparkSession.builder().appName("Spark SQL First Steps").master("local[*]").config("spark.sql.warehouse.dir", "file:///c:/temp/").getOrCreate()) {


            List<Row> inMemory = new ArrayList<Row>();
            inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
            inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
            inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
            inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
            inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

            StructField[] structFields = new StructField[]{
                    new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
            };
            StructType structType = new StructType(structFields);
            Dataset<Row> dataset = spark.createDataFrame(inMemory, structType);
            dataset.show();

            // uncomment for testing:
            //System.out.println("\n===== DEBUG ONLY ! ======\n\tOpen Spark-Jobs page at http://localhost:4040 to analyze execution performande - Press Enter when finished\n=========================\n");
            //new Scanner(System.in).nextLine();
        }

    }
}

