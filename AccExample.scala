
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}
import scala.collection.mutable.{ListBuffer, HashMap => MutableHashMap}

/**
  * This program illustrate creation of user defined Accumulator.
  */

object  CustomizedListAccumulator extends AccumulatorParam[ListBuffer[Int]]{

  override def addInPlace(r1: ListBuffer[Int], r2: ListBuffer[Int]) = {
    r1 ++ r2
  }

  override def zero(initialValue: ListBuffer[Int]) = {
    ListBuffer[Int]()
  }
}

object AccExample{
  def main (args :Array[String]): Unit ={

    val sconf = new SparkConf()
    val sc = new SparkContext()

    val listAcc =sc.accumulator(CustomizedListAccumulator.zero(new ListBuffer[Int]()))(CustomizedListAccumulator)
    val list = (1 to 20) toList
    val rdd = sc.parallelize(list)

    rdd.foreach(x =>  listAcc += ListBuffer(x) )
    println(listAcc.value.mkString(","))
  }
}
