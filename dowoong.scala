package org.example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkException
//////////////////////
//object rnd_result {

//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder.master("local[*]").appName("Random").getOrCreate()
///////////////////////
    val sc = spark.sparkContext

    val a = double2Double(1)
    val b = double2Double(1)
    val w1 = 0.5
    val w2 = 0.5

    val sim = scala.util.Random

    case class myClass(id: Int, SIM: Double, TRUST: Double, Result: Double)
    import spark.implicits._
    val list0 = (1 to 100).map { x =>
      val tid = x
      val tSim = sim.nextDouble()
      val tTrust = sim.nextDouble()
      val tResult = 0
      val res = myClass(tid, tSim, tTrust, tResult)
      res
    }
    val myDF = list0.toDF()
    myDF.show()

    case class myClass2(id: Int, SIM_Result: Double, TRUST_Result: Double, Result: Double)
    val list1 = list0.map { row =>
      val c1 = row.SIM * a * w1
      val c2 = row.TRUST * b * w2
      val c3 = c1 + c2
      val res = myClass2(row.id, c1, c2, c3)
      res
    }

    val myDF2 = list1.toDF().orderBy(desc("Result"))
    myDF2.show()

    val myDF3 = list1.toDF().orderBy(desc("Result")).limit(10)
    myDF3.show()
  }
//}

----------------------------------------------------협업필터링

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

case class Rating(id: Int, SIM_Result: Double, TRUST_Result: Double, Result: Double)
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  assert(fields.size == 4)
  Rating(fields(0).toInt, fields(1).toDouble, fields(2).toDouble, fields(3).toDouble)
}

val ratings = spark.read.myClass2.map(parseRating).toDF()
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

// Build the recommendation model using ALS on the training data
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("id").setItemCol("SIM_Result").setItemCol("TRUST_Result").setRatingCol("Result")
val model = als.fit(training)

// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")
val predictions = model.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)

// Generate top 10 movie recommendations for a specified set of users
val users = ratings.select(als.getUserCol).distinct().limit(3)
val userSubsetRecs = model.recommendForUserSubset(users, 10)
// Generate top 10 user recommendations for a specified set of movies
val movies = ratings.select(als.getItemCol).distinct().limit(3)
val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
