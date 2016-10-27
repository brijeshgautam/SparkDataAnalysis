import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bgautam on 10/27/2016.
  */
object SortByKeyCustomOrdering {
def main(args :Array[String]): Unit ={

  val sconf= new SparkConf().setMaster("local").setAppName("Sort By Key Example")

  val sc = new SparkContext(sconf)

  val list = List((10,100),(3, 40),(1,12),(100,200),(15,30))
  val  rdd = sc.parallelize(list)
  implicit val sortIntegerByString = new Ordering[Int]{
    override def compare(x: Int, y: Int): Int = x.toString.compare(y.toString)
  }
  val sortRdd =rdd.sortByKey()
sortRdd.collect().foreach(println)
}
}
