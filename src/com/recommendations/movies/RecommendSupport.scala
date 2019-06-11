package com.recommendations.movies

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class RecommendSupport {
  
  val sc = new SparkContext("local[*]", "movie recommendation");
  
  // mapping user id with movie ratings 
  //which returns (uId, (mId, rating)) in the form of RDD
  def mapUserIdAndMovieRatings() : RDD[(Int, (Int, Double))] = {
    
    val data_file = sc.textFile("files/u.data")
    
    val userIdMappedWithMovieIdAndRatings : RDD[(Int, (Int, Double))] 
     = data_file.map(line => 
       {
         val fields = line.split("\\s+")    // \s+ is a regular expression for one or more spaces.
         (fields(0).toInt, (fields(1).toInt, fields(3).toDouble))
       }  
     )
    
     userIdMappedWithMovieIdAndRatings  //return type
  }
  
  // filtering duplicate values
  def filterDuplicateMovieData(userIdAndPairOfMovies : (Int, ((Int, Double), (Int, Double)))) : Boolean = {
    
    val mId1 = userIdAndPairOfMovies._2._1._1
    val mId2 = userIdAndPairOfMovies._2._2._1
    
    mId1 < mId2
    // MovieId1==MovieId2 ( Same movie ) and MovieId2 < MovieId1 (Repeated Pair )
    
  }
  
  
  // mapping movie id with ratings by converting into (movie1, movie2) => (rating1, rating2)
  def mapMovieIdWithRatings(uIdAndMovieData : (Int, ((Int, Double),(Int, Double)))) : ((Int, Int), (Double, Double)) = {
    
    val mId1 = uIdAndMovieData._2._1._1
    val mId2 = uIdAndMovieData._2._2._1
    
    val mr1 = uIdAndMovieData._2._1._2
    val mr2 = uIdAndMovieData._2._2._2
    
    ((mId1, mId2), (mr1, mr2))
    
  }
}