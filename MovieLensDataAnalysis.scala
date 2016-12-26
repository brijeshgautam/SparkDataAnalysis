/**
  * Dataset for this project is picked from http://grouplens.org/datasets/movielens/latest/
  * This project solves the following problem :
  *  1)
  *  spark-submit --class MovieLensDataAnalysis  spark2-11_2.11-1.0.jar
  */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

class MovieLensDataAnalysis {
  val spark = SparkSession.builder().appName("MovieLens Data Analysis").getOrCreate()
  lazy val movieDF =readMovieData()
  lazy val ratingDF = readRatingData()



  //For speeding up the things ,we are creating schema explicitly for rating database and movie database.
  def prepareRatingSchema() ={
     StructType(Array(StructField("userId",IntegerType,true),StructField("movieId",IntegerType,true),
        StructField("rating",DoubleType,true)))
  }
  def prepareMovieSchema():StructType ={
    StructType(Array(StructField("ID", IntegerType,true),StructField("title", StringType,true)))
  }

  // Read comma separated movie database,  drop the "genres" column , renamed  "movieId" column. Since
  // database is going to be used frequently , so persist dataframe in the memory.
  // Do same thing with Rating data.
  def readMovieData():DataFrame ={

    val rawMoviesDF = spark.sqlContext.read.format("com.databricks.spark.csv").option("header",true).option("inferSchema",false).
      schema(prepareMovieSchema()).load("hdfs:///user/ml-latest-small/movies.csv")

     val moviesDF = rawMoviesDF.drop("Genres").withColumnRenamed("movieId","ID").persist(StorageLevel.MEMORY_ONLY)

     moviesDF
  }

  def readRatingData():DataFrame ={

    val rawRatingDF = spark.sqlContext.read.format("com.databricks.spark.csv").option("header",true).option("inferSchema",false).
      schema(prepareRatingSchema()).load("hdfs:///user/ml-latest-small/ratings.csv")

     val ratingsDF = rawRatingDF.drop("Timestamp").cache()
     ratingsDF
  }

  def computeHighestAvgRating():DataFrame={
    def computeAvgRating :DataFrame ={
      val movie_ids_with_avg_ratings_df = ratingDF.groupBy("movieId").agg(count("rating").alias("count"), avg("rating").alias("average") )
      println("movie_ids_with_avg_ratings_df:")
      movie_ids_with_avg_ratings_df.show(3, truncate=false)
      val movie_names_df = movie_ids_with_avg_ratings_df.join(movieDF, movieDF.col("ID").equalTo(movie_ids_with_avg_ratings_df.col("movieId")))
      movie_names_df.drop("ID")
    }
    //  Fetch movies with  atleast 100 reviews and in descending order of average rating
    def getHighestAverageRatingWithReviews(df:DataFrame,numReview:Int):DataFrame ={
        df.filter(df.col("count")>= numReview).orderBy(desc("average"))
    }
    val movie_names_with_avg_ratings_df = computeAvgRating
    val df =getHighestAverageRatingWithReviews(movie_names_with_avg_ratings_df, 100)
   // println("showing size of " + df.count())
    df
  }

// SECOND PART
  /*
  Break up the ratings_df dataset into three pieces before applying machine learning:
 A training set (DataFrame), which we will use to train models
 A validation set (DataFrame), which we will use to choose the best model
 A test set (DataFrame), which we will use for our experiments
   */


  lazy val (split_60_df, split_a_20_df, split_b_20_df) = splitDataFrame(ratingDF)
  lazy val training_df = split_60_df.cache()
  lazy val validation_df = split_a_20_df.cache()
  lazy val test_df = split_b_20_df.cache()

  def splitDataFrame(df :DataFrame):(DataFrame,DataFrame,DataFrame) ={
    val seed = 1800009193L
    val splitDF = df.randomSplit(Array(0.60,0.20,0.20), seed)

    (splitDF(0),splitDF(1),splitDF(2))
  }
  def getRegEval()={
     new RegressionEvaluator ().setPredictionCol("prediction").setLabelCol("rating").setMetricName("rmse")
  }
  def getALS() ={
   lazy val seed = 1800009193L
    lazy val  als =new ALS().setMaxIter(5).setSeed(seed).setRegParam(0.1).setRatingCol("rating")
      .setUserCol("userId").setItemCol("movieId")
     als
  }
  def developModel()={
    val als = getALS()
    val reg_eval = getRegEval()
    val tolerance = 0.03
    val ranks = Array(4, 8, 12)
    val errors = Array(0.0, 0.0, 0.0)
    val models = new Array [ALSModel](3)
    var counter = 0
    var min_error = Double.PositiveInfinity
    var best_rank = -1
    for (rank <- ranks )
    {
      als.setRank(rank)
      val model = als.fit(training_df)
      val predict_df = model.transform(validation_df)
      val predicted_ratings_df = predict_df.filter(predict_df.col("prediction").isNotNull)
      val error = reg_eval.evaluate(predicted_ratings_df)
      errors(counter) = error
      models(counter) = model

      min_error = if (error < min_error ){best_rank = counter ; error} else min_error

      counter += 1
    }
    als.setRank(ranks(best_rank))

    System.out.println("The best model was trained with rank "+ ranks(best_rank))
    models (best_rank)
  }

  def computeRMSE(model :ALSModel) = {
     val predict_df = model.transform(test_df)
     val predicted_test_df = predict_df.filter(predict_df.col("prediction").isNotNull)
     getRegEval().evaluate(predicted_test_df)
  }

  def computeRSMEUsingAvg()={
    val  avg_rating_df = training_df.agg(F.avg(training_df.col("rating")))
    //avg_rating_df = training_df.agg(F.avg(training_df.rating))
    //avg_rating_df.collect()

    //Extract the average rating value. (This is row 0, column 0.)
    val training_avg_rating = avg_rating_df.collect()(0)(0)

    System.out.println("The average rating for movies in the training set is " + training_avg_rating)

    //Add a column with the average rating
    val test_for_avg_df = test_df.withColumn("prediction",F.lit(training_avg_rating))
      //withColumn('prediction', F.lit(training_avg_rating))

    // Run the previously created RMSE evaluator, reg_eval, on the test_for_avg_df DataFrame
      val test_avg_RMSE = getRegEval().evaluate(test_for_avg_df)
      test_avg_RMSE
  }

  def doRecommendationForMe(total :Int): Unit ={
    /**
      * create a new DataFrame called my_ratings_df, with our ratings for at least 10 movie ratings.
      * Each entry should be formatted as (my_user_id, movieID, rating). As in the original dataset,
      * ratings should be between 1 and 5 (inclusive) so this new dataframe should contain rating between
      * 1 and 5 .
      *
      */
      val my_user_id = 0
      val myRatedMovies ={
        import org.apache.spark.sql.Row

        List(Row(my_user_id, 260, 5),Row(my_user_id, 318, 3),
          Row(my_user_id, 858, 4),Row(my_user_id, 912, 3),
          Row(my_user_id, 750, 4),Row(my_user_id, 1198, 2),
          Row(my_user_id, 58559, 5),Row(my_user_id, 4226, 3),
          Row(my_user_id, 50, 5), Row(my_user_id, 527, 5))
      }

      def createDataFrameForMyMovie:DataFrame = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(myRatedMovies),
                                                prepareRatingSchema())

      // add your ratings to the training dataset so that the model you train will incorporate your preferences.
      def combineTrainingWithMyRatigDF(my_ratigs_df:DataFrame):DataFrame =training_df.union(my_ratigs_df)

      def prepareDfExclusiveMyRatedMovie() :DataFrame ={
        // Create a list of my rated movie IDs
          val myRatedMoviesID= for (movieTuple  <- myRatedMovies) yield movieTuple(2)
        // Filter out the movies I already rated.
          val notRatedDF = movieDF.filter(row => ! myRatedMoviesID.contains(row.get(0) ))

        // Rename the "ID" column to be "movieId", and add a column with my_user_id as "userId".
          val myUnratedMoviesDf = notRatedDF.withColumnRenamed("ID","movieId").withColumn("userId",F.lit(my_user_id))

          myUnratedMoviesDf.show()
          myUnratedMoviesDf
      }
      val training_with_my_ratings_df= combineTrainingWithMyRatigDF(createDataFrameForMyMovie)
       //Now, train a model with my ratings added and the parameters , which was used in previous model training


      val als = getALS()

      val my_ratings_model = als.fit(training_with_my_ratings_df)
       // Compute the RMSE for this new model on the test set.
      val rsme =computeRMSE(my_ratings_model)
      println("Model with my ratings has rsme =" + rsme)
      // let's predict what ratings you would give to the movies that you did not already provide ratings for.

      val my_unrated_movies_df =prepareDfExclusiveMyRatedMovie()

      //Use my_rating_model to predict ratings for the movies that I did not manually rate.
      val raw_predicted_ratings_df = my_ratings_model.transform(my_unrated_movies_df)

      val predicted_ratings_df = raw_predicted_ratings_df.filter(raw_predicted_ratings_df.col("prediction").isNotNull)
      //We have our predicted ratings. Now we can print out the 25 movies with the highest predicted ratings.
     predicted_ratings_df.printSchema()
    predicted_ratings_df.show(20, truncate = false)
  }
}

object  MovieLensDataAnalysis{
  def main(args: Array[String]): Unit = {

    val obj = new MovieLensDataAnalysis

    println(obj.movieDF.count())
    obj.movieDF.printSchema()
    obj.movieDF.show(30,truncate = false)

    println(obj.ratingDF.count())
    obj.ratingDF.printSchema()
    obj.ratingDF.show(20,truncate = false)


 /*
 One way to recommend movies is to always recommend the movies with the highest average rating.
 In this part, we will use Spark to find the name, number of ratings, and the average rating of
 the 20 movies with the highest average rating and at least 100 reviews.
  */
    val movie_names_with_avg_ratings_df= obj.computeHighestAvgRating()
    movie_names_with_avg_ratings_df.show(20, truncate = false)
   // Develop model using Alternating Least Square technique
    val model =obj.developModel()
    val test_rsme = obj.computeRMSE(model)
    System.out.println("RSME for test dataframe is" + test_rsme)

    //Comparing Your Model
    val avg_rsme =obj.computeRSMEUsingAvg()
    System.out.println("RSME for test dataframe using average value is " + avg_rsme)
   //Predictions for Yourself . This program will predict  50 most rated movies.
    obj.doRecommendationForMe(50)
  }
}
