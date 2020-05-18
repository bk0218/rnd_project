// mongodb collection 제거
// db.USER_SIMILARITY.drop()

var userforSimilarity_df = userforSimilarity_table.select(col("STD_NO"), col("SUBJECT_STAR"), col("NCR_STAR"), col("ACTING_COUNT"))
userforSimilarity_df = userforSimilarity_df.drop("_id")

//질의자
var querySTD_NO = 20142820
var querySTD = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${querySTD_NO}")).drop("STD_NO")

val exStr = "WrappedArray|\\(|\\)|\\]|\\["


var querySTD_List = querySTD.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)
var sbjt_star = querySTD.select(col("SUBJECT_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
var ncr_star = querySTD.select(col("NCR_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
var acting_count = querySTD.select(col("ACTING_COUNT")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)

var w1, w2, w3 = 0.33 //가중치 값
var a, b, r = 1 //알파, 베타, 감마

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
var user_sim_tuples_sbjt = Seq[(String, Double)]()
var user_sim_tuples_ncr = Seq[(String, Double)]()
var user_sim_tuples_act = Seq[(String, Double)]()
var stdNO_inDepart_List = userforSimilarity_df.select(col("STD_NO")).rdd.map(x => x(0)).collect().toList

stdNO_inDepart_List.foreach(stdNO => {
  // var i = 0; //토탈 구해줄 떄 바꿀라고

  println(stdNO)
  //유사사용자
  var std_inDepart = userforSimilarity_df.filter(userforSimilarity_df("STD_NO").equalTo(s"${stdNO}")).drop("STD_NO")
  var std_inDepart_List = std_inDepart.collect.toList.mkString.replaceAll(exStr, "").split(",").map(x => (x.trim.toDouble * 10).toInt)

  //교과
  var sbjt_star_ = std_inDepart.select(col("SUBJECT_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
  //비교과
  var ncr_star_ = std_inDepart.select(col("NCR_STAR")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)
  //자율활동
  var acting_count_ = std_inDepart.select(col("ACTING_COUNT")).collect.toList.mkString.replaceAll(exStr, "").split(",").map(x=>(x.trim.toDouble * 10).toInt)

  //val sim = CosineSimilarity.cosineSimilarity(querySTD_List, std_inDepart_List)
  val sbjt_sim = CosineSimilarity.cosineSimilarity(sbjt_star, sbjt_star_)
  val ncr_sim = CosineSimilarity.cosineSimilarity(ncr_star, ncr_star_)
  val acting_sim = CosineSimilarity.cosineSimilarity(acting_count, acting_count_)

  //println(sim)
  //user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", sim)
  user_sim_tuples_sbjt = user_sim_tuples_sbjt :+ (s"${stdNO}", sbjt_sim)
  user_sim_tuples_ncr = user_sim_tuples_ncr :+ (s"${stdNO}", ncr_sim)
  user_sim_tuples_act = user_sim_tuples_act :+ (s"${stdNO}", acting_sim)

  // var total_sim = (user_sim_tuples_sbjt(i)._2 * w1) + (user_sim_tuples_ncr(i)._2 * w2) + (user_sim_tuples_act(i)._2 * w3)
  var total_sim = (sbjt_sim * w1) + (ncr_sim * w2) + (acting_sim * w3)
  println(total_sim)
  //
  // i += 1;

  //전체 유사도 구하기
  user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", total_sim)

})

var user_sim_df = user_sim_tuples.toDF("STD_NO", "similarity")
var user_sim_sbjt_df = user_sim_tuples_sbjt.toDF("STD_NO", "sbjt_similarity")
var user_sim_ncr_df = user_sim_tuples_ncr.toDF("STD_NO", "ncr_similarity")
var user_sim_acting_df = user_sim_tuples_act.toDF("STD_NO", "acting_similarity")


var join_df_temp1 = user_sim_sbjt_df.join(user_sim_ncr_df, Seq("STD_NO"), "outer")
var join_df_temp2 = join_df_temp1.join(user_sim_acting_df, Seq("STD_NO"), "outer")
val user_sim_join_df = join_df_temp2.join(user_sim_df, Seq("STD_NO"), "outer")

// setMongoDF_USER_SIM(spark, join_df)

MongoSpark.save(
user_sim_join_df.write
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/cpmongo_distinct.USER_SIMILARITY")
    .mode("overwrite"))



//setMongoDF_USER_SIM(spark, user_sim_sbjt_df)
//setMongoDF_USER_SIM(spark, user_sim_ncr_df)
//setMongoDF_USER_SIM(spark, user_sim_acting_df)
//
// def updateField(_id : String, inputDocument : String): Future[UpdateResult] = {
//
//    /* inputDocument = {"key" : value}*/
//
//    val mongoClient = MongoClient("mongodb://localhost:27017")
//    val database: MongoDatabase = mongoClient.getDatabase("cpmongo_distinct")
//    val collection: MongoCollection[Document] = database.getCollection("USER_SIMILARITY")
//
//    val updateDocument = Document("$set" -> Document(inputDocument))
//
//    collection
//      .updateOne(Filters.eq("_id", BsonObjectId(_id)), updateDocument)
//      .toFuture()
//  }







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
