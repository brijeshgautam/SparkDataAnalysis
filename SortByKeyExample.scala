import org.apache.spark.{SparkConf, SparkContext}

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

// output is : (abc:5,20) (abc:10,100) (agh:-1,9) (def:1,-100) (ght:-10,56) (ght:-2,67)
