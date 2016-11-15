/**
  * This log is taken from http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html for month of July.
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.storage.StorageLevel

class LogAnalysis {
  val sconf= new SparkConf().setMaster("local").setAppName("Log Analysis")
  val sc= new SparkContext(sconf)
  val sqlContext =  new SQLContext(sc)

  def createDFForUserDefinedList(): DataFrame ={
    val list =List(Seq("Anthony", 10), Seq("Julia", 20), Seq("Fred", 5))
    val rows = list.map{x => Row(x:_*)}
    val schema = StructType(List(StructField("name",StringType,true),StructField("count",IntegerType,true)))
    val df= sqlContext.createDataFrame(sc.parallelize(rows),schema)
    df
  }

  def readLogFile():DataFrame ={
    val logDF =sqlContext.read.text("hdfs:///user/NASALOGFILE.txt")
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

    println(obj.checkIfAnyColumnForNullVal(cleanedDF))
  }
}
