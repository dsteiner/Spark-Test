package dst.testing.spark;

// example data from udemy.com/course/apache-spark-for-java-developers


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BigDataExcercise {

    public static void main(String[] args) {

        BigDataExcercise joins = new BigDataExcercise();

        joins.findMostPopularCourse();


    }

    /**
     * Find out which courses are most popular based on chapter views.
     * Excercise from https://www.udemy.com/course/apache-spark-for-java-developers
     * See also: src/main/resources/viewing figures/Practical+Guide.pdf
     */
    public void findMostPopularCourse() {
        System.out.println("\nComplete Example: Find Most Popular Course");
        // see src/main/esources/viewing figures/Practical+Guide.pdf
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("(Full) Join Example App");

        boolean testMode = true; // Last Excercise 4 - set to false to read from files

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {

            //You are producing an Apache Spark "report" for a video training company.
            //They have dumped a set of raw data from a database which is capturing the
            //number of views each video has.
            //Here is the raw data:

            //First of all, there is a dump containing "views" - there are two columns as follows
            // NOTE: chapterId is unique within ALL courses
            JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode); // (userId, chapterId)

            //What are chapterIds? We have a separate dump file containing the following
            JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode); // (chapterId, courseId)

            //There is another dump file (titles.csv) containing courseIds against their titles
            JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode); // (courseId,title)

            // Excercise 1: build an RDD containing a key of courseId together with the number of chapters on that course.
            JavaPairRDD<Integer, Long> chapterCountRDD = chapterData
                    .mapToPair(t -> new Tuple2<>(t._2, 1L))
                    .reduceByKey(Long::sum);

            if (testMode) {
                System.out.println("\nChapter Count (courseId, chapterCount):");
                chapterCountRDD.collect().forEach(System.out::println);
            }

            // Excercise 2: produce a ranking chart detailing which are the most popular courses by score

            // 1. Removing Duplicate Views (on viewData)
            //the same user watching the same chapter doesn't count, so we can call distinct to remove duplications.
            JavaPairRDD<Integer, Integer> distinctViewDataRDD = viewData.distinct(); // (userId, chapterId)

            // 2.  Joining to get Course Id in the RDD
            //This isn't very meaningful as it is - we know for example that user 14 has
            //watched 96 and 97, but are they from the same course, or are they different
            //courses???? We need to join the data together. As the common column in
            //both RDDs is the chapterId, we need both Rdds to be keyed on that
            JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterAndTupelOfUserCourseIdRDD = distinctViewDataRDD
                    .mapToPair(t -> new Tuple2<>(t._2, t._1)) // switch to (chapterId, userId)
                    .join(chapterData); // (chapterId, (userId, courseId))
            if (testMode) {
                System.out.println("\nJoined Result to get Course Id -> (chapterId, (userId, courseId)):");
                chapterAndTupelOfUserCourseIdRDD.collect().forEach(System.out::println);
            }

            // 3. Drop the Chapter Id
            //As each "row" in the RDD now represents "a chapter from this course has
            //been viewed", the chapterIds are no longer relevant. You can get rid of them
            //at this point - this will avoid dealing with tricky nested tuples later on. At the
            //same time, given we're counting how many chapters of a course each user
            //watched, we will be counting shortly, so we can drop a "1" in for each
            //user/course combination.
            JavaPairRDD<Tuple2<Integer, Integer>, Long> userIdsCourseIdsAndNumberOfViewsRDD = chapterAndTupelOfUserCourseIdRDD
                    .mapToPair(t -> new Tuple2<>(t._2, 1L))
                    //Step 4 - Count Views for User/Course
                    .reduceByKey(Long::sum ); // run a reduce as usual.
            if (testMode) {
                System.out.println("\nSwitched and reducedByKey -> ((userId, courseId),number of Views):");
                userIdsCourseIdsAndNumberOfViewsRDD.collect().forEach(System.out::println);
                System.out.println("We can read these results as 'user 14 watched 2 chapters of 1; user 13 watched 1 chapter of course 1; user 14 watched 2 chapters of course 2. etc'");
            }

            // 5. Drop the userId
            //The user id is no longer relevant. We've needed it up until now to ensure that
            //for example, 10 users watching 1 chapter doesn't count the same as 1 user
            //watching 10 chapters! Another map transformation should get you...
            JavaPairRDD<Integer, Long> courseIdsNumberOfViewsRDD = userIdsCourseIdsAndNumberOfViewsRDD
                    .mapToPair(t -> new Tuple2<>(t._1._2, t._2));
            if (testMode) {
                System.out.println("\nDropped userIDd -> (courseId,number of Views):");
                courseIdsNumberOfViewsRDD.collect().forEach(System.out::println);
                System.out.println("To rephrase the previous step, we now read this as 'someone watched 2 chapters of course 1. Somebody different watched 1 chapter of course 1;\n someone watched 1 chapter of course 2, etc");
            }

            // 6: add in the total number of chapters for each (courseId, #ofViews) tuple
            //The scoring is based on what percentage of the total course they watched. So
            //we will need to get the total number of chapters for each course into our RDD:

            // Chapter Count (courseId, chapterCount) -> 
            JavaPairRDD<Integer, Tuple2<Long, Long>> courseIdNumberOfViewsOfChapter = courseIdsNumberOfViewsRDD.join(chapterCountRDD);
            if (testMode) {
                System.out.println("\nADD in the total number of chapters of a courseId to the previous tuple -> (courseId,#ofViews of (total number of) chapters):");
                courseIdNumberOfViewsOfChapter.collect().forEach(System.out::println);
                System.out.println("Someone watched 2 chapters of 3. Somebody different watched 1 chapter of 3");
            }

            // 7: Convert to percentages
            JavaPairRDD<Integer, Double> coursePercentage = courseIdNumberOfViewsOfChapter
                    //.mapToPair(t -> new Tuple2<>(t._1, (Double.valueOf(t._2._1) / t._2._2)));
                    // better: don't use Double.valueOf(...) - auto-boxing/unboxing is expensive
                    .mapValues( t->t._1.doubleValue() / t._2);
            if (testMode) {
                System.out.println("\n(courseId,views percentage):");
                coursePercentage.collect().forEach(System.out::println);
            }

            // 8: Convert to scores
            //* If a user watches more than 90% of the course, the course gets 10 points
            //* If a user watches > 50% but <90% , it scores 4
            //* If a user watches > 25% but < 50% it scores 2
            //* Less than 25% is no score
            JavaPairRDD<Integer, Long> courseIdScoreRDD = coursePercentage
                    .mapValues(t -> {
                        if (t > 0.9) return 10L;
                        else if (t > 0.5) return 4L;
                        else if (t > 0.25) return 2L;
                        return 0L;
                    })
                    .reduceByKey(Long::sum); // 9: Add up the total scores
            if (testMode) {
                System.out.println("\n(courseId,score):");
                courseIdScoreRDD.collect().forEach(System.out::println);
            }

            // Exercise 3: Join to get the titles
            JavaPairRDD<Long, String> withTitles = courseIdScoreRDD
                    .join(titlesData)
                    .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
                    .sortByKey(false);

            System.out.println("\n(Titles,score):");
            withTitles.collect().forEach(System.out::println);


        }
    }


    // (courseId,title)
    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
                });
    }

    // (chapterId,courseId)
    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {



        if (testMode) {
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }


        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    // (userId,chapterId)
    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {


        if (testMode) {
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }

}

