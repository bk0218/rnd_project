def TRUST(applyComlist:org.apache.spark.sql.DataFrame, searchComlist:org.apache.spark.sql.DataFrame, gStdComList:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame={
val corps_applyComlist = applyComlist.collect.map(_.toSeq).flatten
val corps_searchComlist = searchComlist.collect.map(_.toSeq).flatten
val corps_gStdComList = gStdComList.collect.map(_.toSeq).flatten

val getGstdInfo = getMongoDF(spark, gradCorpUri) //test
val getGstdInfo_ = getGstdInfo.select("*") //rdd2

def categ(corps_applyComlist: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
     var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
     for(i<-0 until corps_applyComlist.size){
     a = a+(corps_applyComlist(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_applyComlist(i)).collect)
     }
     a}

def categ1(corps_searchComlist: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
     var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
     for(i<-0 until corps_searchComlist.size){
     a = a+(corps_searchComlist(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_searchComlist(i)).collect)
     }
     a}

def categ2(corps_gStdComList: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
     var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
     for(i<-0 until corps_gStdComList.size){
     a = a+(corps_gStdComList(i)->getGstdInfo_.filter(getGstdInfo_("GCI_CORP_NM")===corps_gStdComList(i)).collect)
     }
     a}

val corps1 = categ(corps_applyComlist)
val corps2 = categ1(corps_searchComlist)
val corps3 = categ2(corps_gStdComList)

corps1.size
corps2.size
corps3.size

var final_corps = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()

if(corps1.size >= 10){
	final_corps = corps1
} else if(corps1.size + corps2.size >= 10){
	final_corps = corps1 ++ corps2
	} else{
	final_corps = corps1 ++ corps2 ++ corps3
	}

val test2 = getMongoDF2(spark, "CPS_RATING")
var tuples = Seq[(Int, Double)]()
val t5 = getGstdInfo.select(col("GCI_STD_NO"), col("GCI_CORP_NM")).distinct.toDF

var final_corps_keys = final_corps.keys.toList

//---------------------사용자 신뢰도(재용)---------------------------


for(i<-0 until final_corps.size){
  val a = 0.2
  val b = 0.125
  val w1 = 0.5
  val w2 = 0.5
  val columns = Seq("GCI_STD_NO", "RATING")
  var df = test2.filter(test2("기업명")===final_corps_keys(i)).toDF
  var filter = t5.filter(t5("GCI_CORP_NM").equalTo(final_corps_keys(i))).collect
  if(df.collect.size>0){
    var add = df.select("기업가중치").as[String].collect()(0).toInt*w1*a+df.select("직원수 가중치").as[String].collect()(0).toInt*w2*b
    for(j<-0 until filter.size){
      tuples = tuples :+ (filter(j)(0).toString.toInt, add.toString.toDouble)
      }
    }else{
      for(j<-0 until filter.size){
        tuples = tuples :+ (filter(j)(0).toString.toInt, 0.0)
    }
  }
}

val df = tuples.toDF("GCI_STD_NO", "TRUST")

val test0 = getMongoDF(spark, "CPS_STAR_POINT")
var con = test0.select(col("STAR_KEY_ID"), col("STAR_POINT"))
var con1 = con.groupBy("STAR_KEY_ID").agg(avg("STAR_POINT").alias("STAR_POINT"))

//코드 다 실행하고 결과 출력해보는 거
df.show()
con1.show() //중분류에 대한 신뢰도

//활동 점수 계산
//교과 + 비교과 + 자율활동

val stInfoUri_ = getMongoDF2(spark, stInfoUri)
var st_list = stInfoUri_.limit(5).select("STD_NO").rdd.map(r=>r(0)).collect()

val gradCorpUri_ = getMongoDF2(spark, gradCorpUri)
val clPassUri_ = getMongoDF2(spark, clPassUri)
val ncrStdInfoUri_ = getMongoDF2(spark, ncrStdInfoUri)
val outActUri_ = getMongoDF2(spark, outActUri)
val starpoint_ = getMongoDF2(spark, "CPS_STAR_POINT")
var out_act_score = Seq[(Int, Double)]()

//아래 for문 시간 좀 걸림.
for(i<-0 until st_list.size){ //학생들의 활동 점수 계산을 위해 영역 별로 count를 수행하여 DF로 만들 것
  var sbjt = clPassUri_.filter(clPassUri_("STD_NO")===st_list(i)).toDF
  var ncr = ncrStdInfoUri_.filter(ncrStdInfoUri_("NPS_STD_NO")===st_list(i)).toDF
  var out = outActUri_.filter(outActUri_("OAM_STD_NO")===st_list(i)).toDF

  var stars = starpoint_.filter(starpoint_("STD_NO")===st_list(i)).count //st_list(i) 학생이 교과/비교과에 준 총 별점의 개수

  var total = sbjt.count+ncr.count+out.count //학생이 총 수강/활동한 (교과/비교과)/대외활동의 수
  var act_score = 0

  if(total==0){
    act_score = stars.toInt / (total+1).toInt //활동 점수 계산
  }else{
    act_score = stars.toInt / total.toInt //활동 점수 계산
  }

  out_act_score = out_act_score :+ (st_list(i).toString.toInt, act_score.toString.toDouble)

}

var out_act_score_df = out_act_score.toDF("STD_NO", "ACT_SCORE")

//st_list.size
//st_list(i)

//기업 점수 df와 활동 점수 df를 내부 조인함
val df_out_join = df.join(out_act_score_df, df("GCI_STD_NO")===out_act_score_df("STD_NO"), "inner")


val scores = df_out_join.select(col("STD_NO"), col("TRUST"), col("ACT_SCORE")).collect //GCI_STD_NO를 제외하고 컬럼들 선택

var user_trust = Seq[(Int, Double)]() //최종 신뢰도가 들어갈 시퀀스

for(i<-0 until scores.size){ //코드가 너무 조잡함. 보기 추함. 정리 필요.
  //scores의 1:학번, 2:기업점수, 3:활동점수가 순서대로 들어감.
  var std_no = scores(i)(0)
  var trust = scores(i)(1)
  var act_score = scores(i)(2)
  val w1 = 0.5
  val w2 = 0.5

  user_trust = user_trust :+ (std_no.asInstanceOf[Int], w1*scores(i)(1).asInstanceOf[Double]+w2*scores(i)(2).asInstanceOf[Double]) //여기서 인자를 추가해서 계산해줘야
}

var user_trust_df = user_trust.toDF("STD_NO", "TRUST") //최종 결과를 데이터프레임으로 만듬

user_trust_df
}
