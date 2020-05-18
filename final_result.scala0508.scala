
var std_NO = 20152611

var user_sim_df = calSim(spark, 20152611)
var user_trust_df = TRUST(temp._1, temp._2, temp._3)
def RECOMEMND_RESULT(std_NO:Int, user_sim_df:DataFrame,applyComlist:org.apache.spark.sql.DataFrame, searchComlist:org.apache.spark.sql.DataFrame, gStdComList:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame={

//---------------------------------------------------------------------------------------------------------------------------------------------------------추천결과생성
// user_sim_tuples = user_sim_tuples :+ (s"${stdNO}", total_sim)
// var user_sim_df = user_sim_tuples.toDF("STD_NO", "similarity")
//var user_trust_df = user_trust.toDF("STD_NO", "TRUST") //최종 결과를 데이터프레임으로 만듬
val user_Result_df = user_trust_df.join(user_sim_df, Seq("STD_NO"), "outer")
val user_Result_df_NaN = user_Result_df.na.fill(0.0, List("SIMILARITY")).na.fill(0.0, List("TRUST"))
// val std_arr = user_Result1.map(x=>x._1).toSeq.toList.take(10)
val std_arr = Seq(20190030, 20170063, 20142915, 20152634, 20142824, 20161627)

//val newDf = user_Result_df_NaN.select(col("SIMILARITY").map(col("TRUST")).reduce((c1, c2) => c1 + c2) as "sum")
//val newDf = user_Result_df_NaN.select(col("SIMILARITY").map(col("TRUST")).reduce((c1, c2) => c1 + c2) as "sum")

//유사도+신뢰도 join DF->list변환

var clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pas
//유사도팀이랑 합치면 없애야되는부분 => 중복제거하면 값 1개만불러와짐
var clPassUri_DF = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KOR_NM"), col("SBJT_KEY_CD")).distinct.toDF

//한학생이 들은 수업리스트
//한학생이 들은 수업리스트(질의자)
// var std_NO = 20152611
var sbjtNM_by_stdNO = clPassUri_DF.filter(clPassUri_DF("STD_NO").equalTo(s"${std_NO}")).select(col("SBJT_KEY_CD")).distinct
var sbjtNM_by_stdNO_List = sbjtNM_by_stdNO.rdd.map(r=>r(0)).collect.toList.distinct.map(_.toString)

clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pas
var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
// outActUri_DF.show()

//유사도팀
var outAct_name_temp1 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${std_NO}")).select(col("OAM_STD_NO"), col("OAM_TITLE"), col("OAM_TYPE_CD")).distinct
//3개의 코드만 필터링
var outAct_name_temp2 = outAct_name_temp1.drop("OAM_STD_NO", "OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD01" || $"OAM_TYPE_CD" ==="OAMTYPCD02").distinct
var outAct_name_List = outAct_name_temp2.rdd.map(r=>r(0)).collect.toList

//---------------------자율활동 추천 code list(자격증 CD01, 어학

val outActUri_CD01_arr = std_arr.map{ stdno =>
  val res_CD01 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res_CD01
}.map{ x =>
  val res_CD01 = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" === "OAMTYPCD01").collect.toList.map( x=> x.toString)
  res_CD01
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse


val outActUri_CD02_arr = std_arr.map{ stdno =>
  val res_CD02 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res_CD02
}.map{ x =>
  val res_CD02 = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" ==="OAMTYPCD02").collect.toList.map( x=> x.toString)
  res_CD02
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD03 = std_arr.map{ stdno =>
  val res_CD03 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res_CD03
}.map{ x =>
  val res_CD03 = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD03").collect.toList
  res_CD03
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD03_result = outActUri_CD03.map(x => x._2)

case class OAM_TYPE_CD_CD03(OAM_TYPE_CD: Int, count : Int)
val outActUri_CD03_arr = outActUri_CD03_result.map{ row =>
  val avg = row / std_arr.length
  val res_CD03 = ("OAM_TYPE_CD03", avg)
  res_CD03
}

val outActUri_CD04 = std_arr.map{ stdno =>
  val res_CD04 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res_CD04
}.map{ x =>
  val res_CD04 = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" ==="OAMTYPCD04").collect.toList
  res_CD04
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD04_result = outActUri_CD04.map(x => x._2)

case class OAM_TYPE_CD_CD04(OAM_TYPE_CD: Int, count : Int)
val outActUri_CD04_arr = outActUri_CD04_result.map{ row =>
  val avg = row / std_arr.length
  val res_CD04 = ("OAM_TYPE_CD04", avg)
  res_CD04
}

val outActUri_CD05 = std_arr.map{ stdno =>
  val res_CD05 = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res_CD05
}.map{ x =>
  val res_CD05 = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" ==="OAMTYPCD05").collect.toList
  res_CD05
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val outActUri_CD05_result = outActUri_CD05.map(x => x._2)

case class OAM_TYPE_CD_CD05(OAM_TYPE_CD: Int, count : Int)
val outActUri_CD05_arr = outActUri_CD05_result.map{ row =>
  val avg = row / std_arr.length
  val res_CD05 = ("OAM_TYPE_CD05", avg)
  res_CD05
}



val res_arr = std_sbjt_arr.map { x =>
  val sbjt = x._1
  val filtered_sbjt_arr = reducedDF_con2.filter( x=> x._1 == sbjt)
  val v = if(filtered_sbjt_arr.isEmpty) x._2
  else filtered_sbjt_arr(0)._2 * x._2
  (sbjt, v)
}

//교과목 결과값에 콘텐츠 신뢰도를 곱해 재 랭킹
val PassUri_top5_list = res_arr.sortBy(x => x._2).reverse


var ncrStdInfoUri_stdNO = ncrInfoUri_DF.filter(ncrInfoUri_DF("NPS_STD_NO").equalTo(s"${std_NO}")).select(col("NPI_KEY_ID")).distinct
var ncrStdInfoUri_stdNO_List = ncrStdInfoUri_stdNO.rdd.map(r=>r(0)).collect.toList.distinct.map(_.toString)
////////////////////////////////////////////////////////////////////////////////////////////

val ncrStdInfoUri_arr = std_arr.map{ stdno =>
  val res = ncrInfoUri_DF.filter(ncrInfoUri_DF("NPS_STD_NO").equalTo(s"${stdno}")).filter($"NPS_STATE" === "NCR_T07_P05")
  res
}.map{ x =>
  val res = x.select("NPI_KEY_ID").collect.map( x=> x.toString)
  res
}.flatMap( x=> x)

//질의학생이 들은 과목 중복제거
// val exStr =
val ncr_replace = ncrStdInfoUri_arr.map(x => x.replaceAll(exStr, ""))
val ncr_filter = ncr_replace.diff(ncrStdInfoUri_stdNO_List)

val ncr_filter_arr = ncr_filter.groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

//콘텐츠신뢰도 STAR_KEY_ID와 비교과 NPI_KEY_ID 일치 여부에 따라 값 곱하기
val res_arr2 = ncr_filter_arr.map { x =>
  val ncr = x._1
  val filtered_ncrStdInfoUri = reducedDF_con2.filter( x=> x._1 == ncr)
  val v = if(filtered_ncrStdInfoUri.isEmpty) x._2
  else filtered_ncrStdInfoUri(0)._2 * x._2
  (ncr, v)
}
////---------------------------------교과(sbjt_df), 비교과(ncr_df), 자율활동(act_df) join (Key값 필요) -------------------------------

val maxSize = 5


var colSet = scala.collection.mutable.Set[String]()

val PassUri_top5 = res_arr.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","SBJT_KEY_CD")
val NCR_std_Info_top5 = res_arr2.sortBy(x => x._2).reverse.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","NPI_KEY_ID")
val CD01_top5 = outActUri_CD01_arr.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","OAMTYPCD01")
val CD02_top5 = outActUri_CD02_arr.take(maxSize).zipWithIndex.map(x => (x._2 + 1, x._1._1)).toDF("Rank","OAMTYPCD02")
//val CD03_avg = outActUri_CD03_arr.zipWithIndex.map(x => (x._2 + 1, s"OAMTYPCD0${x._2 + 3}", x._1._2)).toDF("Rank","OAM_TYPE_CD03", "AVG")
val CD03_avg = outActUri_CD03_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD03_AVG")
val CD04_avg = outActUri_CD04_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD04_AVG")
val CD05_avg = outActUri_CD05_arr.zipWithIndex.map(x => (x._2 + 1, x._1._2)).toDF("Rank", "OAMTYPCD05_AVG")

//최종적으로 join해서 합치기
//val rankList1 = Seq(NCR_std_Info_top5, CD01_top5, CD02_top5, CD03_avg)
//val rankList2 = Seq(NCR_std_Info_top5, CD01_top5, CD02_top5, CD03_avg, CD04_avg, CD05_avg)
val rankList = Seq(NCR_std_Info_top5, CD01_top5, CD02_top5, CD03_avg, CD04_avg, CD05_avg)

var Result_All = PassUri_top5
rankList.foreach{ DF =>   Result_All = Result_All.join(DF, Seq("Rank"), "outer")}


setMongoDF_result(spark, Result_All)
print("??")
Result_All.sort("Rank").show
Result_All
}
