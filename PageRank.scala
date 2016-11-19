/**
  * This  program computes  page rank of a nodes in a graph. Input to this program is file which
  * contains adjacency list representation of the graph.
  * Input entry in the file is in following format :
  * 1,6,2,4
  * 9,6,2,1,9
  * Here first entry indicates  source vertex and remaining entries are links of that node.
  */


import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random
import org.apache.hadoop.fs.Path

object PageRank {
  val epsilon =  0.0000001

  val conf = new SparkConf().setAppName("page rank")
  val sc = new SparkContext(conf)

  def computePageRank(graph :RDD[(Int,Array[Int])]): RDD[(Int,Double)] ={
    def  computePageRankHelper(currRank:RDD[(Int, Double)], prevRank:RDD[(Int,Double)]):RDD[(Int,Double)] ={
      //  compare  prevRank and current rank value . If there is no change in the value return current rank value
      if (currRank.join(prevRank).mapValues[Boolean]((valTuple:(Double, Double)) => valTuple._1 - valTuple._2 <= epsilon).map(idRank => idRank._2).
        reduce((rank1, rank2) => rank1 && rank2)== true) currRank

      else {
        val contribution = graph.join(currRank).flatMap { case (source, (links, currRank)) => links.map(dest => (dest, currRank / links.size)) }
        computePageRankHelper(contribution.reduceByKey(_ + _).mapValues(x => 0.15 + 0.85 * x), currRank)
        //ranks = contribution.reduceByKey(_ + _).mapValues(x => 0.15 + 0.85 * x)
        //println(ranks.partitioner) // identify partitioner for ranks

      }
    }

    val rank = graph.mapValues(x => 1.0)
    val prevRank = graph.mapValues(x => 0.0)
    //println(ranks.partitioner) // identify partitioner for ranks
    computePageRankHelper(rank, prevRank )
  }
  def generateGraph( graphFile :String): Unit ={

    val total = 50
    val graph = new mutable.HashMap[Int,Set[Int]]()
    val r = Random
    for ( count <- 1 to total * 5){
      val source = r.nextInt(total +1 )
      val dest = r.nextInt(total +1)
      graph(source) = graph.getOrElse(source, Set()) + dest
    }


    val graphRDD = sc.parallelize(graph.mapValues(_.mkString(",")).map((key) => key._1.toString + "," + key._2).toSeq)
    graphRDD.saveAsTextFile(graphFile)

  }

  def deleteHDFSFile(file1:String, file2:String){
    val hconf =sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hconf)
    if (fs.exists(new Path(file1)))
      fs.delete(new Path(file1), true)

    if (fs.exists(new Path(file2)))
      fs.delete(new Path(file2), true)
  }
  def main (args :Array[String]): Unit ={


    val graphFile ="/user/inputGraph"
    val outputFile ="/user/outputRank"
    deleteHDFSFile(graphFile, outputFile)
    generateGraph(graphFile)
    val graph = sc.textFile("hdfs:///"+ graphFile).map(x => x.split(",").
      map(_.toInt)).map(x => (x(0),x.drop(1))).
      partitionBy(new HashPartitioner(50)).persist()

    val rankValue= computePageRank(graph)

    rankValue.saveAsTextFile(outputFile)

    //sc.stop()
  }
}

