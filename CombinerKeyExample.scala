/** This program illustrates usage of method CombineByKey .
  * spark-submit  --class WordCount  <name-of-jar-file> 
  */

import org.apache.spark.{SparkConf, SparkContext}

/** 
  * spark-submit  --class CombinerKeyExample <name-of-jar-file> 
  */
object CombinerKeyExample {
  def main (args :Array[String]): Unit ={
   type runCollector = (Int,Int)
   type PersonScores = (String,(Int, Int))

   val sconf = new SparkConf().setAppName("CombinerKeyExample").setMaster("local")
   val sc = new SparkContext(sconf)
   val initalRun= Array(("Sachin", 88), ("Sachin", 95), ("Dhoni", 91), ("Dhoni", 93), ("Kohli", 155),("Kohil",78), ("Raina", 40))

    val runSc = sc.parallelize(initalRun).cache()

    val scoreCombiner = (score: Int) => (1, score)

    val mergeValues= (collector: runCollector, score: Int) => {
      val (matches, totalScore) = collector
      ( matches + 1, totalScore + score)
    }

    val mergeCombiner = (collector1: runCollector, collector2: runCollector) => {
      val (matches1, totalScore1) = collector1
      val (matches2, totalScore2) = collector2
      ( matches1 +  matches2, totalScore1 + totalScore2)
    }


    val scores = runSc.combineByKey(scoreCombiner, mergeValues, mergeCombiner)

    val averagingFunction = (personScore: PersonScores) => {
      val (name, (matches, totalRun)) = personScore
      (name, totalRun / matches)
    }

    val averageScores = scores.collectAsMap().map(averagingFunction)

    println("Average Scores using CombingByKey")
    averageScores.foreach((ps) => {
      val(name,average) = ps
      println(name+ "'s average score : " + average)
    })


  }
}

