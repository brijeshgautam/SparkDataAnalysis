import org.apache.spark.{SparkConf, SparkContext}

/** This program computes word count using spark for local file . It also writes output to local file. 
  * spark-submit  --class WordCount  <name-of-jar-file>  <input-file>  <output-file-path>
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

