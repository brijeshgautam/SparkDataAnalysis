/**
  * This program is based on Graphx library of Apache Spark .
  * This program computes the shortest path between any two nodes in the graph. Shortest path computation is done
  * using dijkstra's algorithm .
  * It uses vertex.csv and edges.csv files as input data file.
  *Vertex.csv  : This file contains vertex id and property of each vertex in the comma separated notation.
  *             For example , graph which is mentioned in wiki(link is mentioned in the reference section), entries will
  *             be in the following format:
  *             1,"a"
  *             2,"b"
  *             3,"c"
  *             4,"d"
  *             5,"e"
  *             6,"f"
  * edges.csv: This file contains edge information between any two vertices in the graph.
  *            Entries in this file is in following format : src-vertex-id, dst-vertex-id,distance.
  *            Sample entry is like :
  *           1,2,7
  *           1,3,9
  * Reference:
  * http://kowshik.github.io/JPregel/pregel_paper.pdf
  * http://www.cakesolutions.net/teamblogs/graphx-pregel-api-an-example
  * https://en.wikipedia.org/wiki/Shortest_path_problem
  * http://spark.apache.org/docs/latest/graphx-programming-guide.html
  * http://stackoverflow.com/questions/23700124/how-to-get-sssp-actual-path-by-apache-spark-graphx
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD


object ShortestPathInGraph {

  def  main(args :Array[String]): Unit ={
    val sconf = new SparkConf().setAppName("Graph Example")
    val sc = new SparkContext(sconf)
    val vertexFile = "hdfs:///user/graph/vertex.csv"
    val edgeFile ="hdfs:///user/graph/edges.csv"

    val vertices :RDD[(VertexId,(String))]= sc.textFile(vertexFile).map(_.split(",")).map(x => (x(0).toLong,x(1)))

    val edges :RDD[Edge[Long]] = sc.textFile(edgeFile).map{
      line =>
        val fields = line.split(",")
        Edge(fields(0).toLong , fields(1).toLong, fields(2).toLong)
    }
// Prepare a graph property
    val missing ="missing"
    val graph = Graph(vertices,edges,missing)
    println(graph.vertices.collect().mkString(",") )
    println(graph.edges.collect().mkString("\n"))


    val sourceId: VertexId = 1 // The  source vertex
    // Initialize the graph such that all vertices except the root have distance infinity.
    //Since we want to print path also along the shortest route, so we will store vertices along the shortest path in
    // the List[VertexId].
    val initialGraph : Graph[(Double, List[VertexId]), Long] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId]()) else (Double.PositiveInfinity, List[VertexId]()))

    println(initialGraph.vertices.collect().mkString(","))
    println(initialGraph.edges.collect().mkString("\n"))


    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()))(
      (id, dist, newDist) =>if (dist._1 <= newDist._1) dist else newDist ,
      triplet => {  // Send Message
        if (triplet.srcAttr._1 + triplet.attr < triplet.dstAttr._1)
          Iterator((triplet.dstId ,(triplet.srcAttr._1 + triplet.attr,triplet.srcAttr._2::: List(triplet.srcId))))
        else  Iterator.empty
      },
      (a, b) => if (a._1 <= b._1 ) a else b //merge message
    )
    println(sssp.vertices.collect.mkString("\n"))
    println(sssp.edges.collect().mkString("\n"))
  }
}
