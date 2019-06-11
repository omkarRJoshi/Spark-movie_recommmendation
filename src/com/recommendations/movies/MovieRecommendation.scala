package com.recommendations.movies

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

object MovieRecommendation {
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val rs = new RecommendSupport()
    
    val userIdMappedWithMovieIdAndRating : RDD[(Int,(Int, Double))] = rs.mapUserIdAndMovieRatings()
    
    //Finding movie sets along with the rating given for each particular user 
    val pairOfMovieWatchedBySameUser : RDD[(Int, ((Int, Double), (Int, Double)))] = userIdMappedWithMovieIdAndRating.join(userIdMappedWithMovieIdAndRating)
    
    //Filtering duplicate movie data
    val pairOfMoviesWithoutDuplicates : RDD[(Int, ((Int, Double),(Int, Double)))] = pairOfMovieWatchedBySameUser.filter(rs.filterDuplicateMovieData)
    
    //Mapping user movie data (movie1, movie2) => (rating1, rating2)
    val moviePairAndRatings : RDD[((Int, Int), (Double, Double))] = pairOfMoviesWithoutDuplicates.map(rs.mapMovieIdWithRatings)
    
    //grouping ratings for same movie pair
    val groupOfMoviePairForSameMoviePair : RDD[((Int, Int), Iterable[(Double, Double)])] = moviePairAndRatings.groupByKey()
    
    
  }
  
}