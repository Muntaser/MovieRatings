import java.io.File
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object MovieRatings {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("MovieRatings")

    if (args.length != 2) {
      println("Usage: [usb root directory]/spark/bin/spark-submit --class MovieRatings " +
        "target/scala-*/movielens-als-ssembly-*.jar movieLensHomeDir outputFile")
      sys.exit(1)
    }

    val sc = new SparkContext(conf)
    val movieLensHomeDir = args(0)
    val outputFile = args(1)


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    case class Ratings(userId: Integer, movieId: Integer, rating: Float, timestamp: String)

    val ratingsData = sc.textFile("/tmp/ml-20m/ratings.csv").map(_.split(",").map(elem => elem.trim)).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(x => Ratings(Integer.parseInt(x(0)), Integer.parseInt(x(1)), x(2).toFloat, x(3)))
    //movies
    case class Movies(movieId: Integer, title: String, genres: String)
    val moviesRdd = sc.textFile("/tmp/ml-20m/movies.csv").map(_.split(",").map(elem => elem.trim)).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(x => Movies(Integer.parseInt(x(0)), x(1), x(2)))
    val moviesDataDF = moviesRdd.toDF

    //Partitions
    //val partitionedKV = movieRdd.partitionBy(new HashPartitioner(3))

    //Number of Action movies
    val countsMoviesAction = moviesRdd.filter(movie => movie.genres.toLowerCase.contains("action")).count()

    println("Number of Action movies: " + countsMoviesAction) //2851

    val groupByGenres = moviesRdd.map(movie => movie.genres.trim.split("\\|"))
      .flatMap(x => x)
      .map(x => (x, 1)).reduceByKey(_ + _)
    val sortedGroupByGenres = groupByGenres
      .map { case (k, v) => (v, k) }
      .sortByKey(ascending = false).map(_.swap)
    println("Genres count: " + groupByGenres.count) //20
    println("Genres distribution of movies: " + sortedGroupByGenres.take(20).foreach(println))

    sortedGroupByGenres.saveAsTextFile(outputFile)

    //Movie distribution by year
    val groupByYear = moviesRdd.map(movie => movie.title.trim.takeRight(6).trim)
      .map(x => try {
        Integer.parseInt(x.slice(1, 5))
      } catch {
        case e: Exception => None
      })
      .map(x => (x, 1)).reduceByKey(_ + _)
    println("Number of years movies have been released: " + groupByYear.count) //Number of years movies have been released: 121

    //Sorted movie count by year in descending order (top 10):

    val sortedgroupByYear = groupByYear.map(_.swap).sortByKey(false).map(_.swap)
    val countMovieByYearFreq = sortedgroupByYear.countByKey()
    println("Sorted movie count by year in descending order (top 10):" + sortedgroupByYear.take(10).foreach(println))

    val filterMovieTitlesCount = sc.accumulator(0)
    //  Use accumulators
    val filterMovieTitles = moviesRdd.map(movie => if (movie.title.split(" ")(0).length >= 10) filterMovieTitlesCount += 1)
    println("Movies count with title length > 10: " + filterMovieTitles.count) //27278
    val filterMovieTitlesPattern = moviesRdd.filter(movie => movie.title.split(" ")(0).length >= 10 && movie.title.split(" ")(0).matches("[A-Za-z]+"))
    println("Movies count with title length > 10 and only contains letters: " + filterMovieTitlesPattern.count) //1163

    //Group by movie id from ratings rdd

    val groupByMovieIdRdd = ratingsData.map(rating => (rating.movieId, 1)).reduceByKey(_ + _)
    val groupByMovieIdRatingsRdd = ratingsData.map(rating => (rating.movieId, rating.rating)).reduceByKey(_ + _)
    case class MoviesAvg(movieId: Integer, ratingCount: Integer, ratingAvg: Float)
    val joinedRdd = groupByMovieIdRdd
      .join(groupByMovieIdRatingsRdd)
      .map(x => (x._1, x._2._1, x._2._2 / x._2._1)).map(x => MoviesAvg(x._1, x._2, x._3))
    val movieNameWithAvgRdd = joinedRdd
      .map(movieAvg => (movieAvg.movieId, movieAvg))
      .join(moviesRdd.map(movie => (movie.movieId, movie)))
    case class MoviesNamesWithAvg(movieId: Integer, ratingCount: Integer, ratingAvg: Float, title: String)
    val movieNameWithAvgRddFinal = movieNameWithAvgRdd
      .map(x => (x._1, x._2._1.ratingCount, x._2._1.ratingAvg, x._2._2.title))
      .map(x => MoviesNamesWithAvg(x._1, x._2, x._3, x._4))
    val movieNameWithAvgRddFinalDF = movieNameWithAvgRddFinal.toDF

    //movieNameWithAvgRddFinalDF.sort(desc("ratingAvg")).show(5)

    //movieNameWithAvgRddFinalDF.sort(desc("ratingCount")).show(5)

    //movieNameWithAvgRddFinalDF.where("ratingCount >= 1000").sort(desc("ratingAvg")).show(10)


    //Another way to get to dataframe
    val movieSchema = StructType(Array(StructField("movieId", IntegerType, true),
      StructField("title", StringType, true),
      StructField("genres", StringType, true)))

    val csvLoader = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    //Load with movieSchema
    val moviesDFWithSchema = csvLoader.schema(movieSchema).load("/tmp/ml-20m/movies.csv")

    //Use case class to convert to rdd
    val moviesDS = moviesDFWithSchema.as[Movies]
    val moviesRDDWithSchema = moviesDS.rdd

    //Parquet
    moviesDFWithSchema.write.parquet("movies.parquet")
    val moviesParquet = sqlContext.read.parquet("movies.parquet")
    moviesParquet.registerTempTable("movies")
    sqlContext.sql("SELECT * FROM movies WHERE title.length > 10 limit 10").show


    //Find movies with highest average ratings
    //First create ratings dataframe with schema and create a new dataframe with movie id, number of ratings and average rating
    //Then join with movies dataframe to add movie name
    //Do the same with rdd

    val ratingsSchema = StructType(Array(StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", FloatType, true),
      StructField("timestamp", StringType, true)))

    //Load with ratingsSchema
    val ratingsDFWithSchema = csvLoader.schema(ratingsSchema).load("/tmp/ml-20m/ratings.csv")

    //Use case class to convert to rdd
    val ratingsDS = ratingsDFWithSchema.as[Ratings]
    val ratingsRDDWithSchema = ratingsDS.rdd.cache()


    val groupByMovieId = ratingsDFWithSchema.groupBy("movieId")
    val movieIdWithCountAndAverageRatingsDF = groupByMovieId.agg(count("rating").alias("count"), avg("rating").alias("average"))
    val movieNameWithCountAndAvgRatingDF = movieIdWithCountAndAverageRatingsDF.join(moviesDFWithSchema, Seq("movieId"))
    val movieNamesWithAvgRatings = movieNameWithCountAndAvgRatingDF.select("average", "title", "count", "movieId")


    val sortByAvg = movieNamesWithAvgRatings.sort(desc("average"))
    sortByAvg.show(5)

    val sortByCount = movieNamesWithAvgRatings.sort(desc("count"))
    sortByCount.show(10)

    //Movies with highest count with at least 60000 reviews
    val moviesWithAtLeast200Ratings = movieNamesWithAvgRatings.where("count >= 60000").sort(desc("count"))
    moviesWithAtLeast200Ratings.show(10)

    val moviesWithAtLeast200RatingsSortByAvg = movieNamesWithAvgRatings.where("count >= 1000").sort(desc("average"))
    moviesWithAtLeast200RatingsSortByAvg.show(10)
    //Top rated movie genres: ()

    val joinedMovieRatingsRdd = movieNameWithCountAndAvgRatingDF.rdd
    val ratingsByGenres = joinedMovieRatingsRdd
      .map(row => (row.getDouble(2), row.getString(4).trim.split("\\|")))
      .flatMapValues(x => x).map(_.swap)
      .mapValues(value => (value, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues { case (sum, count) => (1.0 * sum) / count }
    val ratingRank = ratingsByGenres
      .map(_.swap)
      .sortByKey(false).map(_.swap).take(20)
    println("Top ranked movie genres: " + ratingRank.take(20).foreach(println))

    //On or After year 2000
    val filterYear = joinedMovieRatingsRdd
      .filter(row =>
        try {
          Integer.parseInt(row.getString(3).trim.takeRight(6).trim.slice(1, 5)) >= 2000
        }
        catch {
          case e: Exception => false
        })
    val ratingsByGenresMillenial = filterYear
      .map(row => (row.getDouble(2), row.getString(4).trim.split("\\|")))
      .flatMapValues(x => x).map(_.swap)
      .mapValues(value => (value, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues { case (sum, count) => (1.0 * sum) / count }
    val ratingRankMillenial = ratingsByGenresMillenial
      .map(_.swap)
      .sortByKey(false)
      .map(_.swap).take(20) //takeOrdered is expensive
    println("Top ranked movie genres after 2000: " + ratingRankMillenial.take(20).foreach(println))

    //statistics of movie ratings
    val ratingStats = ratingsData.map(ratings => ratings.rating).stats()
    println(ratingStats)
    val moviesCount = moviesRdd.count //20000263

    sc.stop()

  }











