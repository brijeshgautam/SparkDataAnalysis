import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * This  program computes  page rank of a nodes in a graph. Input to this program is file which
  * contains adjacency list representation of the graph.
  * Input entry in the file is in following format :
  * 1,6,2,4
  * 9,6,2,1,9
  * Here first entry indicates  source vertex and remaining entries are links of that node. 
  */
object PageRank {

  def computePageRank(graph :RDD[(Int,Array[Int])]): RDD[(Int,Double)] ={
    val keys = graph.map(x => x._1).max()
    //println("keys " + keys)

    var ranks = graph.mapValues(x => 1.0)

   for ( count <- 1 to 100) {
      val contribution = graph.join(ranks).flatMap { case (source, (links, currRank)) => links.map(dest => (dest, currRank / links.size)) }
      ranks = contribution.reduceByKey(_ + _).mapValues(x => 0.15 + 0.85 * x)
    }
    ranks

  }
  def main (args :Array[String]): Unit ={

    val conf = new SparkConf().setAppName("page rank").setMaster("local")
    val sc = new SparkContext(conf)
    val graph = sc.textFile("file:///"+args(0)).map(x => x.split(",").
      map(_.toInt)).map(x => (x(0),x.drop(1))).
      partitionBy(new HashPartitioner(50)).persist()

    val rankValue= computePageRank(graph)
    rankValue.collect().foreach(println)
    new PrintWriter("pageRankOutput.txt"){write(rankValue.collect().mkString("\n")); close}

  }
}
