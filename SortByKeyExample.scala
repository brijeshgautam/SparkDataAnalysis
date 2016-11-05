import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bgautam on 11/5/2016.
  */

case class Sample(str :String, data :Int) extends Ordered[Sample]{
  override def compare(that: Sample): Int = if(str equals(that.str)) data compare(that.data) else str compare(that.str)
  override def toString: String = str + ":"+ data.toString
}

object SortByKeyExample {

  def main (args :Array[String]): Unit ={
    val sconf = new SparkConf().setMaster("local").setAppName("sortKeyExample")
    val sc = new SparkContext(sconf)
    val sampleTupleList =List((Sample("abc",10),100), (Sample("def",1),-100),(Sample("abc",5),20), (Sample("agh",-1),9),(Sample("ght",-2),67), (Sample("ght",-10),56))
    val inputRDD = sc.parallelize(sampleTupleList)
    implicit val sampleSorting= new Ordering[Sample]{
      override def compare(x: Sample, y: Sample): Int = x compare(y)
    }

    val sortSample = inputRDD.sortByKey(true,4)
    sortSample.collect().foreach(println)
  }

}
