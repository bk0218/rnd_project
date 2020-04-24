// mongodb collection 제거
// db.USER_SIMILARITY.drop()

var userforSimilarity_df = userforSimilarity_table.select(col("STD_NO"), col("SUBJECT_STAR"), col("NCR_STAR"), col("ACTING_COUNT"))
userforSimilarity_df = userforSimilarity_df.drop("_id")

//질의자
var querySTD_NO = 20142820
var querySTD = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${querySTD_NO}")).drop("STD_NO")

val exStr = "WrappedArray|\\(|\\)|\\]|\\["
var querySTD_List = querySTD.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

object CosineSimilarity {
   def dotProduct(x: Array[Int], y: Array[Int]): Int = {
     (for((a, b) <- x zip y) yield a * b) sum
   }
   def magnitude(x: Array[Int]): Double = {
     math.sqrt(x map(i => i*i) sum)
   }
  def cosineSimilarity(x: Array[Int], y: Array[Int]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
}

var user_sim_tuples = Seq[(String, Double)]()
var stdNO_inDepart_List = userforSimilarity_df.select(col("STD_NO")).rdd.map(x => x(0)).collect().toList

stdNO_inDepart_List.foreach(stdNO => {
  println(stdNO)
  //유사사용자
  var std_inDepart = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${stdNO}")).drop("STD_NO")
  var std_inDepart_List = std_inDepart.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

  val sim = CosineSimilarity.cosineSimilarity(querySTD_List, std_inDepart_List)
  println(sim)
  user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", sim)
})

val user_sim_df = user_sim_tuples.toDF("STD_NO", "similarity")
setMongoDF_USER_SIM(spark, user_sim_df)


// querySTD_List
// Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 41, 0, 0, 0, 0, 0, 0, 44, 34, 33, 36, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 45, 0, 38, 42, 40, 38, 0, 0, 0, 10, 0, 10, 10)
//
// std_inDepart_List
// Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//   0, 0, 0, 0, 0, 0, 35, 30, -10, 27, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//   0, 0, 0, 0, 0, 0, 0, 0, 0, 31, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 41, 36, 0, 0, 0, 40, 27, 34, 20, 0,
//   20, 0, 0)
