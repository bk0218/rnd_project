import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.sql._
import org.bson.Document
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column
import org.apache.log4j.Logger
import org.apache.log4j.Level

// val base ="mongodb://127.0.0.1/cpmongo."

val base ="mongodb://127.0.0.1/cpmongo_distinct."
val output_base = "mongodb://127.0.0.1/cpmongo_distinct.USER_SIMILARITY"
val SREG_output_base = "mongodb://127.0.0.1/cpmongo_distinct.SREG_SIM"
val NCR_output_base = "mongodb://127.0.0.1/cpmongo_distinct.NCR_SIM"
val ACT_output_base = "mongodb://127.0.0.1/cpmongo_distinct.ACTIVITY_SIM"
//교과: SREG_SIM, 비교과: NCR_SIM, 자율활동: ACTIVITY_SIM
val Result_output_base = "mongodb://127.0.0.1/cpmongo_distinct.Recommend_Result"


val replyUri = "CPS_BOARD_REPLY"  //댓글
val codeUri = "CPS_CODE_MNG"  //통합 코드관리 테이블
val gradCorpUri = "CPS_GRADUATE_CORP_INFO"  //졸업 기업
val ncrInfoUri = "CPS_NCR_PROGRAM_INFO"  //비교과 정보
val ncrStdInfoUri = "CPS_NCR_PROGRAM_STD"  //비교과 신청학생
val outActUri = "CPS_OUT_ACTIVITY_MNG"  //교외활동
val jobInfoUri = "CPS_SCHOOL_EMPLOY_INFO"  //채용정보-관리자 등록
val sjobInfoUri = "CPS_SCHOOL_EMPLOY_STD_INFO"  //채용정보 신청 학생 정보(student job info)


val deptInfoUri = "V_STD_CDP_DEPT"  //학과 정보 (department info)
val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
val stInfoUri = "V_STD_CDP_SREG"  //학생 정보 (student info)
val pfInfoUri = "V_STD_CDP_STAF"  //교수 정보 (professor info)
val clInfoUri = "V_STD_CDP_SUBJECT"  //교과 정보 (class info)

val cpsStarUri = "CPS_STAR_POINT"  //교과/비교과용 별점 테이블
val userSimilarityUri = "USER_SIMILARITY" //유사도 분석 팀이 생성한 테이블

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
Logger.getLogger("MongoRelation").setLevel(Level.OFF)
Logger.getLogger("MongoClientCache").setLevel(Level.OFF)

def getMongoDF(
                spark : SparkSession,
                coll : String ) : DataFrame = {
  spark.read.mongo(ReadConfig(Map("uri"->(base+coll))))
}

//새로 수정(연희 수정)
def setMongoDF(
                spark : SparkSession,
                df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(output_base))))
}

//setMongoDF(spark, dataframe명)
val base ="mongodb://127.0.0.1/cpmongo."
val base2 = "mongodb://127.0.0.1/cpmongo_distinct."

def getMongoDF(
                spark : SparkSession,
                coll : String ) : DataFrame = {
  spark.read.mongo(ReadConfig(Map("uri"->(base+coll))))
}

def getMongoDF2(
                 spark : SparkSession,
                 coll : String ) : DataFrame = {
  spark.read.mongo(ReadConfig(Map("uri"->(base2+coll))))
}

//저장하기 setMongo(spark, Uri, dataframe)으로 사용
def setMongoDF(
                spark : SparkSession,
                coll: String,
                df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(base+coll))))
}

//추천결과팀 데이터 저장
def setMongoDF_result(
                       spark : SparkSession,
                       df : DataFrame ) = {
  df.saveToMongoDB(WriteConfig(Map("uri"->(Result_output_base))))
}

//setMongoDF_result(spark, dataframe명)

//테이블 명세서 참고
val replyUri = "CPS_BOARD_REPLY"  //댓글
val codeUri = "CPS_CODE_MNG"  //통합 코드관리 테이블
val gradCorpUri = "CPS_GRADUATE_CORP_INFO"  //졸업 기업
val ncrInfoUri = "CPS_NCR_PROGRAM_INFO"  //비교과 정보
val ncrStdInfoUri = "CPS_NCR_PROGRAM_STD"  //비교과 신청학생
val outActUri = "CPS_OUT_ACTIVITY_MNG"  //교외활동
val jobInfoUri = "CPS_SCHOOL_EMPLOY_INFO"  //채용정보-관리자 등록
val sjobInfoUri = "CPS_SCHOOL_EMPLOY_STD_INFO"  //채용정보 신청 학생 정보(student job info)

val deptInfoUri = "V_STD_CDP_DEPT"  //학과 정보 (department info)
val clPassUri = "V_STD_CDP_PASSCURI" //교과목 수료(class pass)
val stInfoUri = "V_STD_CDP_SREG"  //학생 정보 (student info)
val pfInfoUri = "V_STD_CDP_STAF"  //교수 정보 (professor info)
val clInfoUri = "V_STD_CDP_SUBJECT"  //교과 정보 (class info)

val test = getMongoDF2(spark, gradCorpUri)

val rdd = test.select("GCI_CORP_NM") //로우로 해당 컬럼들 읽어옴
val rdd2 = test.select("*")

val corps = rdd.collect.distinct.map(_.toSeq).flatten //읽어온 로우들 한 Seq에 박는 거

def shit(corps: Array[Any]):scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]={
  var a = scala.collection.mutable.Map[Any, Array[org.apache.spark.sql.Row]]()
  for(i<-0 until corps.size){
    a = a+(corps(i)->rdd2.filter(rdd2("GCI_CORP_NM")===corps(i)).collect)
  }
  a}

val corps1 = shit(corps)

//기업 가중치 읽어오는 거
val test2 = getMongoDF(spark, "CPS_RATING")
var tuples = Seq[(Int, Double)]()
val t5 = test.select(col("GCI_STD_NO"), col("GCI_CORP_NM")).distinct.toDF

// (사용자 신뢰도)
for(i<-0 until corps.size){
  val a = 0.2
  val b = 0.125
  val w1 = 0.5
  val w2 = 0.5
  val columns = Seq("GCI_STD_NO", "RATING")
  var df = test2.filter(test2("기업명")===corps(i)).toDF
  var filter = t5.filter(t5("GCI_CORP_NM").equalTo(corps(i))).collect
  if(df.collect.size>0){
    var add = df.select("기업가중치").as[String].collect()(0).toInt*w1*a+df.select("직원수 가중치").as[String].collect()(0).toInt*w2*b
    for(j<-0 until filter.size){
      //      tuples = tuples :+ (filter(j)(0).toString.toInt, add.toString.toDouble)
      tuples = tuples :+ (filter(j)(0).toString.toInt, add.toString.asInstanceOf[Double])
    }
  }else{
    for(j<-0 until filter.size){
      tuples = tuples :+ (filter(j)(0).toString.toInt, 0.0)
    }
  }
}
//val df = tuples.toDF("GCI_STD_NO", "TRUST")

//콘텐츠 신뢰도
val test0 = getMongoDF(spark, "CPS_STAR_POINT")
var con = test0.select(col("STAR_KEY_ID"), col("STAR_POINT"))
var con1 = con.groupBy("STAR_KEY_ID").agg(avg("STAR_POINT").alias("STAR_POINT"))

//코드 다 실행하고 결과 출력해보는 거
df.show()
con1.show()

//예전꺼
// def setMongoDF(
// spark : SparkSession,
// coll: String,
// df : DataFrame ) = {
// df.saveToMongoDB(WriteConfig(Map("uri"->(base+coll))))
// }

val replyUri_table = getMongoDF(spark, replyUri)  //댓글
val codeUri_table =  getMongoDF(spark, codeUri)  //통합 코드관리 테이블
val gradCorpUri_table =  getMongoDF(spark, gradCorpUri)  //졸업 기업
val ncrInfoUri_table =  getMongoDF(spark, ncrInfoUri)  //비교과 정보
val ncrStdInfoUri_table =  getMongoDF(spark, ncrStdInfoUri)  //비교과 신청학생
val outActUri_table =  getMongoDF(spark, outActUri)  //교외활동
val jobInfoUri_table =  getMongoDF(spark, jobInfoUri)  //채용정보-관리자 등록
val sjobInfoUri_table =  getMongoDF(spark, sjobInfoUri)  //채용정보 신청 학생 정보(student job info)


val deptInfoUri_table =  getMongoDF(spark, deptInfoUri)  //학과 정보 (department info)
val clPassUri_table =  getMongoDF(spark, clPassUri) //교과목 수료(class pass)
val stInfoUri_table =  getMongoDF(spark, stInfoUri)  //학생 정보 (student info)
val pfInfoUri_table =  getMongoDF(spark, pfInfoUri)  //교수 정보 (professor info)
val clInfoUri_table =  getMongoDF(spark, clInfoUri)  //교과 정보 (class info)

val cpsStarUri_table = getMongoDF(spark, cpsStarUri)  //교과/비교과용 별점 테이블
val userSimilarity_table = getMongoDF(spark, userSimilarityUri) //유사도 분석 팀이 생성한 테이블

val a = double2Double(1)
val b = double2Double(1)
val w1 = 0.5
val w2 = 0.5

val sim = scala.util.Random

case class myClass(STD_NO: Int, SIM: Double, TRUST: Double, Result: Double)
import spark.implicits._
val list0 = tuples.map { x =>
  val STD_NO = x._1
  val tSim = sim.nextDouble()
  val tTrust = x._2
  val tResult = 0
  val res = myClass(STD_NO, tSim, tTrust, tResult)
  res
}
val myDF = list0.toDF()
myDF.show()

//가중치(w), 상수(a,b), 최종추천 학생 계산부분
case class myClass2(STD_NO: Int, SIM_Result: Double, TRUST_Result: Double, Result: Double)
val list1 = list0.map { row =>
  val c1 = row.SIM * a * w1
  val c2 = row.TRUST * b * w2
  val c3 = c1 + c2
  val res = myClass2(row.STD_NO, c1, c2, c3)
  res
}

val myDF2 = list1.toDF().orderBy(desc("Result"))
myDF2.show()

val myDF3 = list1.toDF().orderBy(desc("Result")).limit(10)
myDF3.show()

//DF -> List로 변경
import org.apache.spark.sql.functions.collect_list
var reducedDF = myDF3.select("STD_NO", "Result").distinct()

reducedDF
  .groupBy("STD_NO")
  .agg(collect_list($"Result").as("Result"))
  .rdd
  .map(row => (row(0).toString -> row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toList))
  .collectAsMap()

reducedDF.groupBy("STD_NO").agg(collect_list($"Result").as("Result")).rdd.map(row => (row(0).toString -> row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toList)).collectAsMap()

// 교과목수료 테이블 중 학번, 학과, 교과목번호, 과목명
var clPassUri_res = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KEY_CD"), col("SBJT_KOR_NM")).distinct.toDF
clPassUri_res.show()

val std_arr = Seq(20190030, 20170063, 20142915, 20152634, 20142824, 20161627)
val std_sbjt_arr = std_arr.map{ stdno =>
  val res = clPassUri_res.filter(clPassUri_res("STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("SBJT_KEY_CD").collect.toList.map( x=> x.toString)
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

//콘텐츠 신뢰도 DF->list변환(reduceDF_con1)
import org.apache.spark.sql.functions.collect_list
var reducedDF_con1 = con1.select("STAR_KEY_ID", "STAR_POINT").distinct()

reducedDF_con1.groupBy("STAR_KEY_ID").agg(collect_list($"STAR_POINT").as("STAR_POINT")).rdd.map(row => (row(0).toString -> row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toList)).collectAsMap().map(x => (x._1, x._2.map(x => x.toFloat)))

//콘텐츠 신뢰도 형변환
val reducedDF_con2 = reducedDF_con1.groupBy("STAR_KEY_ID").agg(collect_list($"STAR_POINT").as("STAR_POINT")).rdd.map(row => (row(0).toString -> row(1).toString)).collect.map{ x =>
  val sbjt = x._1
  val starPoint = x._2.slice(13,x._2.length-1).toDouble
  (sbjt, starPoint)
}

//교과목수료 테이블 형변환
val std_sbjt_arr2 = std_sbjt_arr.map{ x =>
  val k = x._1
  val key = k.slice(1, k.length-1)
  (key, x._2)
}

//콘텐츠신뢰도 STAR_KEY_ID와 교과목수료 SBJT_KEY_CD일치 여부에 따라 값 곱하기
val res_arr = std_sbjt_arr2.map { x =>
  val sbjt = x._1
  val filtered_sbjt_arr = reducedDF_con2.filter( x=> x._1 == sbjt)
  val v = if(filtered_sbjt_arr.isEmpty) x._2
  else filtered_sbjt_arr(0)._2 * x._2
  (sbjt, v)
}

//교과목 결과값에 콘텐츠 신뢰도를 곱해 재 랭킹
val PassUri_top5 = res_arr.sortBy(x => x._2).reverse.take(5)


//비교과 신청학생 테이블 중 학번, 비교과 프로그램 학생키아이디, 비교과 프로그램 학생키아이디, 과목명
//NPS_STATE 코드 (NCR_T07_P00 : 대기신청, NCR_T07_P01 : 승인대기, NCR_T07_P02 :승인, NCR_T07_P03 : 학생취소,
//NCR_T07_P04 : 관리자취소, NCR_T07_P05 : 이수, NCR_T07_P06 : 미이수, NCR_T07_P07 : 반려)

var ncrStdInfoUri_res = ncrStdInfoUri_table.select(col("NPS_STD_NO"), col("NPS_KEY_ID"), col("NPI_KEY_ID"), col("NPS_STATE")).distinct.toDF
ncrStdInfoUri_res.show()

// 데이터 학번 20142820, 20142932, 20152611, 20152615
//  val dfs_2 = Seq(std_STAR_KEY_list1, std_STAR_KEY_list2, std_STAR_KEY_list3, std_STAR_KEY_list4)
val std_arr2 = Seq(201937001, 20142932, 20152611, 20152615, 201926041)
val ncrStdInfoUri_arr = std_arr2.map{ stdno =>
  val res = ncrStdInfoUri_res.filter(ncrStdInfoUri_res("NPS_STD_NO").equalTo(s"${stdno}")).filter($"NPS_STATE" === "NCR_T07_P05")
  res
}.map{ x =>
  val res = x.select("NPI_KEY_ID").collect.toList.map( x=> x.toString).distinct
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

//비교과 신청학생 테이블 형변환
val ncrStdInfoUri_arr2 = ncrStdInfoUri_arr.map{ x =>
  val k = x._1
  val key = k.slice(1, k.length-1)
  (key, x._2)
}

//콘텐츠신뢰도 STAR_KEY_ID와 비교과 NPI_KEY_ID 일치 여부에 따라 값 곱하기
val res_arr2 = ncrStdInfoUri_arr2.map { x =>
  val ncr = x._1
  val filtered_ncrStdInfoUri = reducedDF_con2.filter( x=> x._1 == ncr)
  val v = if(filtered_ncrStdInfoUri.isEmpty) x._2
  else filtered_ncrStdInfoUri(0)._2 * x._2
  (ncr, v)
}

val NCR_std_Info_top5 = res_arr2.sortBy(x => x._2).reverse.take(5)


//-------------------- # # # 자율활동 리스트 # # # ------------------------------
//from.교외활동 CPS_OUT_ACTIVITY_MNG : 학번(OAM_STD_NO), 활동구분코드(OAM_TYPE_CD), 활동명(OAM_TITLE)
//자격증(CD01) : 이름(OAM_TITLE) / ex. 토익800~900, FLEX 일본어 2A,  FLEX 일본어 1A,  FLEX 중국어 1A
//어학(CD02) : 이름(OAM_TITLE)
//봉사(CD03), 대외활동(CD04), 기관현장실습(CD05) : 활동구분코드(OAM_TYPE_CD)

// 자율활동 테이블 중 학번, 학과, 교과목번호, 과목명
var outActUri_DF = outActUri_table.select(col("OAM_STD_NO"), col("OAM_TYPE_CD"), col("OAM_TITLE"))
outActUri_DF.show()

val OAM_STD_NO = Seq(201937027,201926086,201937040)


//---------------------자율활동 추천 code list(자격증 CD01, 어학 CD02)----------------------

val outActUri_CD01 = OAM_STD_NO.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" === "OAMTYPCD01").collect.toList.map( x=> x.toString)
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse
val CD01_top5 = outActUri_CD01.take(2)

val outActUri_CD02 = OAM_STD_NO.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TITLE").filter($"OAM_TYPE_CD" ==="OAMTYPCD02").collect.toList.map( x=> x.toString)
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val CD02_top5 = outActUri_CD02.take(2)

//---------------------자율활동 추천 code list(봉사 CD03, 대외활동 CD04, 기관현장실습 CD05)----------------------


val outActUri_CD03 = OAM_STD_NO.map{ stdno =>
  val res = outActUri_DF.filter(outActUri_DF("OAM_STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("OAM_TYPE_CD").filter($"OAM_TYPE_CD" === "OAMTYPCD03" || $"OAM_TYPE_CD" ==="OAMTYPCD04" || $"OAM_TYPE_CD" ==="OAMTYPCD05").collect.toList
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val result = outActUri_CD03.map(x => x._2)

case class myClass3(OAM_TYPE_CD: Int, count : Int)
val list3 = result.map{ row =>
  val avg = row / OAM_STD_NO.length
  val res3 = ("OAM_TYPE_CD", avg)
  res3
}