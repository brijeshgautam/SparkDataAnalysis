import org.apache.spark.{SparkConf, SparkContext}
import  org.apache.spark.rdd.RDD
/**
  * This program computes  average  of list of integer using mapPartition.
  */
object MapPartitionExample {

  def partitionCtr(iterator: Iterator[Int]) ={
    var sumCount = (0,0)
     for (x  <- iterator){
       sumCount=  (sumCount._1 + x , sumCount._2 + 1)
     }
    List(sumCount).iterator
  }
  def combineCtr(tuple1 :(Int,Int),tuple2 :(Int,Int) )={
    (tuple1._1 + tuple2._1 ,tuple1._2 + tuple2._2)
  }
  def fastAvg(rdd :RDD[Int]) :Double ={
    val retValue =rdd.mapPartitions(partitionCtr _).reduce(combineCtr _)
    retValue._1/retValue._2.toDouble

  }
  def main (args :Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("Map Partition example")
    val sc = new SparkContext(conf)

    val list = (for ( count <- 1 to 10000) yield count).toList

    val listRdd= sc.parallelize(list, 10)
    println(fastAvg(listRdd))
  }
}
