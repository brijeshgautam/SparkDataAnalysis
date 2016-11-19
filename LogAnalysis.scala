/**
  * This log is taken from http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html for month of July.
  * This project solved the following problems:
  * 1) Create Schema for  List, display schema and apply select transformation and show the result.
  * 2) Load NASA HTTP log file, parse log file and create dataframe from it.
  * 3) Do ETL operation on missing column values.
  * 4) After ETL is done, parse timestamp ( which is in string format) and convert into actual time datatype.
  * 5) Persist the Dataframe after step 4.
  * 6) Compute some statistics about the sizes of content being returned by the web server.
  * 7) Compute host that appear more than 10 times in the log.
  * 8) Compute the top ten paths which did not have return code 200.
  * 9) Compute Unique hosts in the entire log file.
  * 10) Determine number of Unique hosts in the entire log on a day-by-day basis.
  * 11) Compute average Number of Daily Requests per Host.
  * 12) Print out a list up to 40 distinct paths that generate 404 errors.
  * 13) print out  a list of the top twenty paths that generate the most 404 errors.
  * 14) Compute the hosts that encountered 404 errors.
  * 15) Compute 404 Errors per Day Basis.
  *
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{split, udf, dayofmonth}
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.storage.StorageLevel

class LogAnalysis extends Serializable{

  //val spark = SparkSession.builder().master("local").appName("Spark Session for Log Analysis").getOrCreate()
  val spark = SparkSession.builder().appName("Spark Session for Log Analysis").getOrCreate()

  def createDFForUserDefinedList(): DataFrame ={
    val list =List(Seq("Anthony", 10), Seq("Julia", 20), Seq("Fred", 5))
    val rows = list.map{x => Row(x:_*)}
    val schema = StructType(List(StructField("name",StringType,true),StructField("count",IntegerType,true)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    df
  }

  def readLogFile():DataFrame ={
    val logDF =spark.read.text("hdfs:///user/NASALOGFILE.txt")
    logDF
  }
  def printInfoOfDataFrame(df :DataFrame): Unit ={
    df.printSchema()
    // dump some entry from dataframe
    df.show(truncate = false)
  }

  def parseLogDataFrame(df:DataFrame):DataFrame ={
    val splitLogDF =df.select(regexp_extract(df.col("value"), "^([^\\s]+\\s)", 1).alias("host"),
      regexp_extract(df.col("value"), "^.*\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4}).*\\]", 1).alias("timestamp"),
      regexp_extract(df.col("value"), "^.*\"\\w+\\s+([^\\s]+)\\s+HTTP.*\"",1).alias("Path"),
      regexp_extract(df.col("value"),"^.*\"\\s+([^\\s]+)",1).cast("Int").alias("Status"),
      regexp_extract(df.col("value"),"^.*\\s+(\\d+)$",1).cast("Int").alias("content_size"))
    splitLogDF
  }
  def checkIfAnyColumnForNullVal(df :DataFrame):BigInt ={
    val bad_rows_df = df.filter(df.col("host").isNull ||
      df.col("timestamp").isNull ||
      df.col("Path").isNull||
      df.col("Status").isNull ||
      df.col("content_size").isNull)
    bad_rows_df.count()
  }

  def findColumnForNullVal(df:DataFrame) = {
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.functions.sum
    def count_null(colName :String) = sum(col(colName).isNull.cast("Int")).alias(colName)

    val expr = df.columns.map(colName => sum(col( colName).isNull.cast("Int") ).alias(colName))
    println(expr)
    df.agg(expr.head, expr.drop(1):_*).show()
  }

  def checkContentSizeforInvalidValue(df:DataFrame) {
    val  bad_content_size_df = df.filter(!df.col("value").rlike("\\d+$"))
    println(bad_content_size_df.count())
    bad_content_size_df.show(5,truncate=false)
  }

  def fillInvalidContentSizeWithZero(df:DataFrame):DataFrame ={
    // Fill content_size with null value as zero.
    df.na.fill(0,Array("content_size"))
  }

  // now that we have a clean, parsed DataFrame, we have to parse the timestamp field into an actual timestamp.
  // The Common Log Format time is somewhat non-standard. A User-Defined Function (UDF) is the most straightforward way to parse it.
  def parseTimestamp(df:DataFrame):DataFrame = {

    def parse_clf_time(str :String):java.sql.Date =
    {
      val f = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
      new java.sql.Date(f.parse(str).getTime)
    }

    val u_parse_time = udf(parse_clf_time _)
    df.select(((for (x <- df.columns ) yield df.apply(x)) :+ u_parse_time(df.col("timestamp")).alias("time") ): _*).drop("timestamp")
  }

  def computeAndShowStats(df :DataFrame): Unit = {
    val content_size_summary_df = df.describe("content_size")
    content_size_summary_df.show()
  }

  def computeStatusStats(df:DataFrame): Unit ={
    val status_to_count_df =df.groupBy("Status").count().sort("Status").cache()
    val status_to_count_length = status_to_count_df.count()
    println("Found "+ status_to_count_length.toString + " response codes ")
    status_to_count_df.show(truncate=false)
  }

  def findHostAccessMoreThanTenTimes(df:DataFrame): Unit ={
    //we create a new DataFrame by grouping successLogsDF by the 'host' column and aggregating by count.
    //We then filter the result based on the count of accesses by each host being greater than ten.
    // Then, we select the 'host' column and show 20 elements from the result.
    val host_sum_df =df.groupBy("host").count()
    val host_more_than_10_df = host_sum_df.filter(host_sum_df.col("count") > 10).select(host_sum_df.col("host"))
    println("Any 20 hosts that have accessed more then 10 times:\n")
    host_more_than_10_df.show(truncate=false)
  }

  def findTopTenPath(df :DataFrame): Unit ={
    val not200DF = df.filter("Status != 200")

    //not200DF.show(10)
    // Sorted DataFrame containing all paths and the number of times they were accessed with non-200 return code
    val logs_sum_df = not200DF.groupBy("Path").count()
    //.orderBy( Seq(col("count")): _*)
    //orderBy(col("count").desc)
    val logs_sum_order_df =logs_sum_df.orderBy(logs_sum_df.col("count").desc)
    println("Top Ten failed URLs:")
    logs_sum_order_df.show(10, false)
  }

  def findUniqueHost(df:DataFrame): Unit ={
    val unique_host_count = df.select("host").distinct().count()

    println("Unique hosts: " + unique_host_count.toString)
  }

  def findUniqueHostOnDailyBasis(df :DataFrame): DataFrame ={
    val day_to_host_pair_df = df.select(df.col("host"), org.apache.spark.sql.functions.dayofmonth(df.col("time")).alias("day")).cache()
    //day_to_host_pair_df.show()
    val day_group_hosts_df = day_to_host_pair_df.distinct()
    //val day_group_hosts_df = day_to_host_pair_df.distinct()
    val daily_host_unnamed_df= day_group_hosts_df.groupBy("day").count()
    daily_host_unnamed_df.printSchema()
    val daily_hosts_df = daily_host_unnamed_df.select(daily_host_unnamed_df.col("day") ,daily_host_unnamed_df.col("count").
      alias("uniqueCount")).cache()
    //select(col("day"),col("count").alias("uniqueCount")).cache()
    daily_hosts_df.show(30, false)
    daily_hosts_df
  }

  def computeAvgDailyReqPerHost(logsDF:DataFrame,uniqHostDF:DataFrame): DataFrame ={
    val total_req_per_day_df = (logsDF.select(logsDF.col("host"), org.apache.spark.sql.functions.dayofmonth(logsDF.col("time")).
      alias("day"))).groupBy("day").count().orderBy("day")

    val joinedDF =total_req_per_day_df.join(uniqHostDF,"day")

    val avg_daily_req_per_host_df = joinedDF.select(joinedDF.col("day"), joinedDF.col("count")/joinedDF.col("uniqueCount")
     as "avgCount").orderBy(joinedDF.col("day"))cache()

    avg_daily_req_per_host_df

  }

  def compute404Responses(df:DataFrame):DataFrame={
    df.filter("status = 404").cache()
  }

  def computeUniquePathFor404(notFoundDF:DataFrame): Unit ={
    val not_found_paths_df = notFoundDF.select("path")
    val unique_not_found_paths_df = not_found_paths_df.distinct()
    unique_not_found_paths_df.show(40, truncate=false)
  }


  def computeTopPathsGenerating404(notFoundDF :DataFrame):DataFrame ={
    val groupedDF =notFoundDF.groupBy("path").count()
      groupedDF.orderBy(groupedDF.col("count").desc)
  }
  def computeAndShowHostFacing404(notFoundDF: DataFrame): Unit ={
     val hosts404groupedDF = notFoundDF.groupBy("host").count()

      val hosts_404_count_df =hosts404groupedDF.orderBy(hosts404groupedDF.col("count").desc)

      println("Top 25 hosts that generated errors:" )
    hosts_404_count_df.show(25, truncate = false
    )
  }
  def computeAndShow404PerDayBasis(notFoundDF:DataFrame): Unit = {
     val errors_by_date_df = notFoundDF.select(notFoundDF.col("status"),dayofmonth(notFoundDF.col("time")).
       alias("day")).groupBy("day").count()

    val errors_by_date_sorted_df =errors_by_date_df.orderBy(errors_by_date_df.col("day")).cache()
     println("404 Errors by day:")
     errors_by_date_sorted_df.show()
   }
  }


object LogAnalysis{
  def main (args :Array[String]): Unit ={

    val  obj = new LogAnalysis

    val userDFForList = obj.createDFForUserDefinedList()

    // Print Schema for userDefine schema for list
    userDFForList.printSchema()
    userDFForList.show()

    // Display usage of count
    println(userDFForList.count())

    // Do Select transformation
    userDFForList.select("name").show

    //  Load apache Log file . This file is already loaded into HDFS at location /user
    val baseDF =obj.readLogFile()
    obj.printInfoOfDataFrame(baseDF)

    // Parse the log File
    val parseLogDF= obj.parseLogDataFrame(baseDF)
    obj.printInfoOfDataFrame(parseLogDF)

    // Data Cleaning. This is part of ETL.
    //let's verify that there are no null rows in the original data set
    if( baseDF.filter(baseDF.col("value").isNull).count() == 0){
      println("No Null rows")
    } else {
      println("there are some null row. Existing the application.")
      System.exit(0)
    }
    // check if any column has null value

    println(obj.checkIfAnyColumnForNullVal(parseLogDF))

    // Since this value is not null , So it means that some columns have null value. lets find out which columns have null value.
    println(obj.findColumnForNullVal(parseLogDF))

    // they're all in the content_size column. Let's see if we can figure out what's wrong.
    //Is it possible there are lines without a valid content size?
    obj.checkContentSizeforInvalidValue(baseDF)

    // The bad rows correspond to error results, where no content was sent back and the server emitted a "-" for the content_size field.
    // Since we don't want to discard those rows from our analysis, let's map them to 0.
    val cleanedDF =obj.fillInvalidContentSizeWithZero(parseLogDF)

    // Check if there are any nulls left.
    println("checking for if any null value is ")
    println(obj.findColumnForNullVal(cleanedDF))

    // now that we have a clean, parsed DataFrame, we have to parse the timestamp field into an actual timestamp.
    // The Common Log Format time is somewhat non-standard. A User-Defined Function (UDF) is the most straightforward way to parse it.
    obj.printInfoOfDataFrame(cleanedDF)
    val logsDF= obj.parseTimestamp(cleanedDF)

    // Since logsDF is going to be used quite often ,so lets cache it here.
    logsDF.persist(StorageLevel.MEMORY_ONLY)
    println(logsDF.count())
    obj.printInfoOfDataFrame(logsDF)

    /**
      * Now that we have a DataFrame containing the parsed log file as a set of Row objects, we can perform various analyses.
      */
    //Content Size Statistics :Let's compute some statistics about the sizes of content being returned by the web server.
    //In particular, we'd like to know what are the average, minimum, and maximum content sizes.

    obj.computeAndShowStats(logsDF)

    // HTTP Status Analysis
    //We want to know which status values appear in the data and how many times.
    obj.computeStatusStats(logsDF)

    //Frequent Hosts
    //Let's look at hosts that have accessed the server frequently (e.g., more than ten times).
    obj.findHostAccessMoreThanTenTimes(logsDF)

    //Top Ten Error Paths
    //What are the top ten paths which did not have return code 200? Create a sorted list containing the paths and the
    // number of times that they were accessed with a non-200 return code and show the top ten.
    obj.findTopTenPath(logsDF)

    //How many unique hosts are there in the entire log?
    obj.findUniqueHost(logsDF)

    //Number of Unique Daily Hosts
    //let's determine the number of unique hosts in the entire log on a day-by-day basis.
    // This computation will give us counts of the number of unique daily hosts.
    // We'd like a DataFrame sorted by increasing day of the month which includes the day of
    // the month and the associated number of unique hosts for that day
    val uniqHostDF =obj.findUniqueHostOnDailyBasis(logsDF)
    obj.printInfoOfDataFrame(uniqHostDF)
   //Average Number of Daily Requests per Host
    //Next, let's determine the average number of requests on a day-by-day basis.
    // We'd like a list by increasing day of the month and the associated average number of requests per host for that day.
   val avgDailyReqHostDF =obj.computeAvgDailyReqPerHost(logsDF,uniqHostDF)
   obj.printInfoOfDataFrame(avgDailyReqHostDF)

    //Counting 404 Response Codes
    val notFoundDF =obj.compute404Responses(logsDF)
    println("Found " + notFoundDF.count() + " URLS")
    obj.printInfoOfDataFrame(notFoundDF)

    //Using notFoundDf, print out a list up to 40 distinct paths that generate 404 errors.
   obj.computeUniquePathFor404(notFoundDF)

    //Listing the Top Twenty 404 Response Code paths
    //Using the DataFrame containing only log records with a 404 response code , print out
    // a list of the top twenty paths that generate the most 404 errors.

    val top_20_not_found_df = obj.computeTopPathsGenerating404(notFoundDF)
    obj.printInfoOfDataFrame(top_20_not_found_df)

    //Listing the Top Twenty-five 404 Response Code Hosts
   //let's look at the hosts that encountered 404 errors. Using the DataFrame containing only log records with
    // a 404 status codes , print out a list of the top twenty-five hosts that generate the most 404 errors.
   obj.computeAndShowHostFacing404(notFoundDF)

   //Listing 404 Errors per Day
   //Let's explore the 404 records temporally. Break down the 404 requests by day and get the daily counts sorted by day
   obj.computeAndShow404PerDayBasis(notFoundDF)

  }
}
