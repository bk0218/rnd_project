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
import org.example.org

// val base ="mongodb://127.0.0.1/cpmongo."

val base ="mongodb://127.0.0.1/cpmongo_distinct."
val output_base = "mongodb://127.0.0.1/cpmongo_distinct.USER_SIMILARITY"
val SREG_output_base = "mongodb://127.0.0.1/cpmongo_distinct.SREG_SIM"
val NCR_output_base = "mongodb://127.0.0.1/cpmongo_distinct.NCR_SIM"
val ACT_output_base = "mongodb://127.0.0.1/cpmongo_distinct.ACTIVITY_SIM"
//교과: SREG_SIM, 비교과: NCR_SIM, 자율활동: ACTIVITY_SIM


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
val list0 = (1 to 100).map { x =>
  val STD_NO = x
  val tSim = sim.nextDouble()
  val tTrust = sim.nextDouble()
  val tResult = 0
  val res = myClass(STD_NO, tSim, tTrust, tResult)
  res
}
val myDF = list0.toDF()
myDF.show()

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
var reducedDF = myDF3.select("STD_NO", "Result").distinct()
import org.apache.spark.sql.functions.collect_list
reducedDF
  .groupBy("STD_NO")
  .agg(collect_list($"Result").as("Result"))
  .rdd
  .map(row => (row(0).toString -> row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toList))
  .collectAsMap()

reducedDF.groupBy("STD_NO").agg(collect_list($"Result").as("Result"))

  .rdd.map(row => (row(0).toString -> row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toList)).collectAsMap()

// 교과목수료 테이블 중 학번, 학과, 교과목번호, 과목명
var clPassUri_res = clPassUri_table.select(col("STD_NO"), col("SUST_CD_NM"), col("SBJT_KEY_CD"), col("SBJT_KOR_NM")).distinct.toDF
clPassUri_res.show()

val std_arr = Seq(20190030, 20170063, 20142915, 20152634, 20142824)
val std_sbjt_arr = std_arr.map{ stdno =>
  val res = clPassUri_res.filter(clPassUri_res("STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("SBJT_KOR_NM").collect.toList.map( x=> x.toString)
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val PassUri_top5 = std_sbjt_arr.take(5)


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
val avg = sum(result) / OAM_STD_NO.count

import sqlContext.implicits._

case class myClass3(OAM_TYPE_CD: Int, count : Int)
val list3 = result.map{ row =>
  val avg = row / OAM_STD_NO.length
  val res3 = ("OAM_TYPE_CD", avg)
  res3
}

//비교과  테이블 중 학번, 학과, 교과목번호, 과목명

var cpsStarUri_res = cpsStarUri_table.select(col("STD_NO"), col("STAR_KEY_ID")).distinct.toDF
cpsStarUri_res.show()

// 데이터 학번 20142820, 20142932, 20152611, 20152615
//  val dfs_2 = Seq(std_STAR_KEY_list1, std_STAR_KEY_list2, std_STAR_KEY_list3, std_STAR_KEY_list4)
val std_arr2 = Seq(20142820, 20142932, 20152611, 20152615)
val std_STAR_KEY_arr2 = std_arr2.map{ stdno =>
  val res = cpsStarUri_res.filter(cpsStarUri_res("STD_NO").equalTo(s"${stdno}"))
  res
}.map{ x =>
  val res = x.select("STAR_KEY_ID").collect.toList.map( x=> x.toString).distinct
  res
}.flatMap( x=> x).groupBy(x => x).mapValues(_.length).toList.sortBy(x => x._2).reverse

val top5 = std_STAR_KEY_arr2.take(5)