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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class SparkSQLQueriesOnMockData {

    public static void main(String[] args) {

        String INPUT_FILENAME = "src/main/resources/logfile/biglog.txt"; // more data - after a while of testing

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

            // load mock data from memory
            Dataset<Row> dataset = spark.createDataFrame(inMemory, structType);

            // load big log file
            /*
            Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema", true).csv(INPUT_FILENAME);
            */

            dataset.show();

            dataset.createOrReplaceTempView("logTable");

            // https://spark.apache.org/docs/3.5.6/sql-ref-functions-builtin.html#aggregate-functions
            /*
                Dataset<Row> resultDataset = spark.sql("select level, count(datetime) from logTable group by level");
             */
            /*
                Dataset<Row> resultDataset = spark.sql("select level, collect_list(datetime) from logTable group by level");
                resultDataset.show();
             */

            /*
            Dataset<Row> resultDataset = spark.sql("select level, date_format(datetime, 'MMMM')as month from logTable");
            resultDataset.createOrReplaceTempView("logTable"); // note: could be same table name as above - its just a temporary view name
            Dataset<Row> resultDataset2 = spark.sql("select level, month, count(1) from logTable group by level, month");
            resultDataset2.show();
            */
            // or in a oneliner
            Dataset<Row> resultDataset = spark.sql("select level, date_format(datetime, 'MMMM')as month, count(1) " +
                    " from logTable group by level, month order by first(date_format(datetime, 'MM')), level"); // could also be in select as new column
            // or "cast(first(date_format(datetime, 'MM')) as int)"

            resultDataset.show(100);

            /*
            dataset.createOrReplaceTempView("result_table");
            Dataset<Row> resultCount = spark.sql("select count(1) from result_table");
            resultCount.show();
            */

            // as column expressions
            //dataset = dataset.selectExpr("level", "date_format(datetime,'MMMM') as month");
            // or as a dataset query
            dataset = dataset.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"), date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
            dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();

            dataset = dataset.orderBy(col("monthnum"), col("level"));
            dataset = dataset.drop("monthnum");

            dataset.show();


            // uncomment for testing:
            //System.out.println("\n===== DEBUG ONLY ! ======\n\tOpen Spark-Jobs page at http://localhost:4040 to analyze execution performande - Press Enter when finished\n=========================\n");
            //new Scanner(System.in).nextLine();
        }

    }
}

