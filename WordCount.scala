import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-submit  --class WordCount  project1_2.11-1.0.jar  inputBrijesh  outputBrijesh
  */
object WordCount {
def main (args :Array[String]): Unit ={

  println("input file is " + args(0))
  println("outputfile1 is " + args(1))
  val conf = new SparkConf().setAppName("wordCount").setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile("file:///root/"+args(0))
  val wordsRDD = input.flatMap(_.split(" "))
  val countRDD = wordsRDD.map((word:String) => ( word , 1)).reduceByKey((x:Int, y:Int) => x + y)
  countRDD.saveAsTextFile("file:///root/"+args(1))
}
}

